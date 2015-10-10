package org.embulk.filter.left_outer_join_json_table;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.TypeDeserializer;
import org.embulk.spi.type.Types;
import org.joda.time.DateTimeZone;
import org.jruby.ir.operands.Hash;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.*;

public class LeftOuterJoinJsonTableFilterPlugin
        implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(LeftOuterJoinJsonTableFilterPlugin.class);

    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("base_column")
        public ColumnConfig getBaseColumn();

        @Config("counter_column")
        @ConfigDefault("{name: id, type: long}")
        public ColumnConfig getCounterColumn();

        @Config("joined_columns_prefix")
        @ConfigDefault("\"_joined_by_embulk_\"")
        public String getJoinedColumnsPrefix();

        @Config("json_file_path")
        public String getJsonFilePath();

        @Config("json_columns")
        public List<ColumnConfig> getJsonColumns();

        @Config("time_zone")
        @ConfigDefault("\"UTC\"")
        public String getTimeZone();

        public HashMap<String, HashMap<String, String>> getJsonTable();
        public void setJsonTable(HashMap<String, HashMap<String, String>> jsonTable);

    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        try {
            task.setJsonTable(buildJsonTable(task, loadJsonFile(task.getJsonFilePath())));
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        Schema outputSchema = buildOutputSchema(task, inputSchema, task.getJsonColumns());

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema, final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        final List<Column> outputColumns = outputSchema.getColumns();
        final List<Column> inputColumns = inputSchema.getColumns();

        Map<String, Column> inputColumnMap = Maps.newHashMap();
        final List<Column> jsonColumns = new ArrayList<>();
        for (Column column : outputColumns) {
            if (!inputColumns.contains(column)) {
                jsonColumns.add(column);
            } else {
                inputColumnMap.put(column.getName(), column);
            }
        }

        final Map<String, ColumnConfig> timestampJsonColumnMap = Maps.newHashMap();
        for (ColumnConfig jsonColumnConfig: task.getJsonColumns()) {
            if (Types.TIMESTAMP.equals(jsonColumnConfig.getType())) {
                timestampJsonColumnMap.put(task.getJoinedColumnsPrefix() + jsonColumnConfig.getName(), jsonColumnConfig);
            }
        }

        final Column baseColumn = inputColumnMap.get(task.getBaseColumn().getName());
        final HashMap<String, HashMap<String, String>> jsonTable = task.getJsonTable();

        return new PageOutput()
        {
            private PageReader reader = new PageReader(inputSchema);

            @Override
            public void finish()
            {
                output.finish();
            }

            @Override
            public void close()
            {
                output.close();
            }

            @Override
            public void add(Page page)
            {
                reader.setPage(page);

                try (final PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output)) {
                    while (reader.nextRecord()) {
                        for (Column inputColumn: inputColumns) {
                            if (reader.isNull(inputColumn)) {
                                builder.setNull(inputColumn);
                                continue;
                            }
                            if (Types.STRING.equals(inputColumn.getType())) {
                                builder.setString(inputColumn, reader.getString(inputColumn));
                            }
                            else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                                builder.setBoolean(inputColumn, reader.getBoolean(inputColumn));
                            }
                            else if (Types.DOUBLE.equals(inputColumn.getType())) {
                                builder.setDouble(inputColumn, reader.getDouble(inputColumn));
                            }
                            else if (Types.LONG.equals(inputColumn.getType())) {
                                builder.setLong(inputColumn, reader.getLong(inputColumn));
                            }
                            else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                                builder.setTimestamp(inputColumn, reader.getTimestamp(inputColumn));
                            }
                        }

                        if (reader.isNull(baseColumn)) {
                            for (Column jsonColumn: jsonColumns) {
                                builder.setNull(jsonColumn);
                            }
                        }
                        else {
                            String rowKey = null;
                            if (Types.STRING.equals(baseColumn.getType())) {
                                rowKey = reader.getString(baseColumn);
                            }
                            else if (Types.BOOLEAN.equals(baseColumn.getType())) {
                                rowKey = String.valueOf(reader.getBoolean(baseColumn));
                            }
                            else if (Types.DOUBLE.equals(baseColumn.getType())) {
                                rowKey = String.valueOf(reader.getDouble(baseColumn));
                            }
                            else if (Types.LONG.equals(baseColumn.getType())) {
                                rowKey = String.valueOf(reader.getLong(baseColumn));
                            }
                            else if (Types.TIMESTAMP.equals(baseColumn.getType())) {
                                rowKey = String.valueOf(reader.getTimestamp(baseColumn));
                            }


                            for (Column jsonColumn: jsonColumns) {
                                String value = jsonTable.get(rowKey).get(jsonColumn.getName());
                                if (value == null) {
                                    builder.setNull(jsonColumn);
                                    continue;
                                }
                                if (Types.STRING.equals(jsonColumn.getType())) {
                                    builder.setString(jsonColumn, value);
                                }
                                else if (Types.BOOLEAN.equals(jsonColumn.getType())) {
                                    builder.setBoolean(jsonColumn, Boolean.parseBoolean(value));
                                }
                                else if (Types.DOUBLE.equals(jsonColumn.getType())) {
                                    builder.setDouble(jsonColumn, Double.parseDouble(value));
                                }
                                else if (Types.LONG.equals(jsonColumn.getType())) {
                                    builder.setLong(jsonColumn, Long.parseLong(value));
                                }
                                else if (Types.TIMESTAMP.equals(jsonColumn.getType())) {
                                    ColumnConfig timestampJsonColumnConfig = timestampJsonColumnMap.get(jsonColumn.getName());
                                    String format = timestampJsonColumnConfig.getOption().get(String.class, "format");
                                    DateTimeZone timezone = DateTimeZone.forID(task.getTimeZone());
                                    TimestampParser parser = new TimestampParser(task.getJRuby(), format, timezone);
                                    builder.setTimestamp(jsonColumn, parser.parse(value));
                                }
                            }
                        }

                        builder.addRecord();
                    }
                    builder.finish();
                }
            }

        };

    }

    private Schema buildOutputSchema(final PluginTask task, Schema inputSchema, List<ColumnConfig> jsonColumns)
    {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        int i = 0;
        for (Column inputColumn: inputSchema.getColumns()) {
            Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
            builder.add(outputColumn);
        }
        for (ColumnConfig columnConfig: jsonColumns) {
            String columnName = task.getJoinedColumnsPrefix() + columnConfig.getName();
            builder.add(new Column(i++, columnName, columnConfig.getType()));
        }
        return new Schema(builder.build());
    }

    private List<HashMap<String, String>> loadJsonFile(String jsonFilePath)
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(jsonFilePath), new TypeReference<ArrayList<HashMap<String, String>>>(){});
    }

    private HashMap<String, HashMap<String, String>> buildJsonTable(final PluginTask task, List<HashMap<String, String>> jsonData)
    {
        HashMap<String, HashMap<String, String>> jsonTable = new HashMap<>();

        for (HashMap<String, String> json: jsonData) {

            HashMap<String, String> record = new HashMap<>();

            for (ColumnConfig columnConfig: task.getJsonColumns()) {
                String columnKey = task.getJoinedColumnsPrefix() + columnConfig.getName();
                String value = json.get(columnConfig.getName());

                record.put(columnKey, value);
            }

            String rowKey = json.get(task.getCounterColumn().getName());
            jsonTable.put(rowKey, record);
        }

        return jsonTable;
    }

}

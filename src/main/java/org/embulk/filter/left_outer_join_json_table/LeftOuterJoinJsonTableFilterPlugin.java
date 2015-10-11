package org.embulk.filter.left_outer_join_json_table;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Civitaspo on 10/10/15.
 */

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

        // create jsonColumns/baseColumn
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

        final Column baseColumn = inputColumnMap.get(task.getBaseColumn().getName());

        // create timestampParserMap
        final HashMap<String, TimestampParser> timestampParserMap = Maps.newHashMap();
        for (ColumnConfig jsonColumnConfig: task.getJsonColumns()) {
            if (Types.TIMESTAMP.equals(jsonColumnConfig.getType())) {
                String format = jsonColumnConfig.getOption().get(String.class, "format");
                DateTimeZone timezone = DateTimeZone.forID(task.getTimeZone());
                TimestampParser parser = new TimestampParser(task.getJRuby(), format, timezone);
                timestampParserMap.put(task.getJoinedColumnsPrefix() + jsonColumnConfig.getName(), parser);
            }
        }

        // get jsonTable
        final HashMap<String, HashMap<String, String>> jsonTable = task.getJsonTable();

        return new LeftOuterJoinJsonTableFilterFilteredPageOutput(inputSchema, outputSchema, baseColumn, jsonTable, jsonColumns, timestampParserMap, output);
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
        HashMap<String, HashMap<String, String>> jsonTable = Maps.newHashMap();

        for (HashMap<String, String> json: jsonData) {

            HashMap<String, String> record = Maps.newHashMap();

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

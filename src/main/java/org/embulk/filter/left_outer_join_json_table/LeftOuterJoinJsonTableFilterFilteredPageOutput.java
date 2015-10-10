package org.embulk.filter.left_outer_join_json_table;

import com.google.common.base.Throwables;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;

import java.util.HashMap;
import java.util.List;

/**
 * Created by Civitaspo on 10/10/15.
 */
public class LeftOuterJoinJsonTableFilterFilteredPageOutput
    implements PageOutput
{
    private final org.slf4j.Logger logger = Exec.getLogger(LeftOuterJoinJsonTableFilterFilteredPageOutput.class);
    private final PageReader pageReader;
    private final PageOutput pageOutput;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final Column joinBaseColumn;
    private final HashMap<String, HashMap<String, String>> jsonTable;
    private final List<Column> jsonColumns;
    private final HashMap<String, TimestampParser> timestampParserMap;

    LeftOuterJoinJsonTableFilterFilteredPageOutput(
            Schema inputSchema,
            Schema outputSchema,
            Column joinBaseColumn,
            HashMap<String, HashMap<String, String>> jsonTable,
            List<Column> jsonColumns,
            HashMap<String, TimestampParser> timestampParserMap,
            PageOutput pageOutput
            )
    {
        this.pageReader = new PageReader(inputSchema);
        this.pageOutput = pageOutput;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.joinBaseColumn = joinBaseColumn;
        this.jsonTable = jsonTable;
        this.jsonColumns = jsonColumns;
        this.timestampParserMap = timestampParserMap;
    }

    @Override
    public void add(Page page)
    {
        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput)) {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                setInputValue(pageBuilder);
                setJoinedJsonValue(pageBuilder);
                pageBuilder.addRecord();
            }
            pageBuilder.finish();
        }

    }

    @Override
    public void finish()
    {
        pageOutput.finish();
    }

    @Override
    public void close()
    {
        pageReader.close();
        pageOutput.close();
    }

    private void setInputValue(PageBuilder pageBuilder) {
        for (Column inputColumn: inputSchema.getColumns()) {
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(inputColumn);
                continue;
            }

            if (Types.STRING.equals(inputColumn.getType())) {
                pageBuilder.setString(inputColumn, pageReader.getString(inputColumn));
            }
            else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                pageBuilder.setBoolean(inputColumn, pageReader.getBoolean(inputColumn));
            }
            else if (Types.DOUBLE.equals(inputColumn.getType())) {
                pageBuilder.setDouble(inputColumn, pageReader.getDouble(inputColumn));
            }
            else if (Types.LONG.equals(inputColumn.getType())) {
                pageBuilder.setLong(inputColumn, pageReader.getLong(inputColumn));
            }
            else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                pageBuilder.setTimestamp(inputColumn, pageReader.getTimestamp(inputColumn));
            }
        }
    }

    private void setJoinedJsonValue(PageBuilder pageBuilder) {
        for (Column jsonColumn: jsonColumns) {
            // get value from JSON Table
            String rowKey = getCurrentJoinBaseColumnValue(pageReader, joinBaseColumn);
            if (rowKey == null) {
                pageBuilder.setNull(jsonColumn);
                continue;
            }

            String value = jsonTable.get(rowKey).get(jsonColumn.getName());
            if (value == null) {
                pageBuilder.setNull(jsonColumn);
                continue;
            }

            if (Types.STRING.equals(jsonColumn.getType())) {
                pageBuilder.setString(jsonColumn, value);
            }
            else if (Types.BOOLEAN.equals(jsonColumn.getType())) {
                pageBuilder.setBoolean(jsonColumn, Boolean.parseBoolean(value));
            }
            else if (Types.DOUBLE.equals(jsonColumn.getType())) {
                pageBuilder.setDouble(jsonColumn, Double.parseDouble(value));
            }
            else if (Types.LONG.equals(jsonColumn.getType())) {
                pageBuilder.setLong(jsonColumn, Long.parseLong(value));
            }
            else if (Types.TIMESTAMP.equals(jsonColumn.getType())) {
                TimestampParser parser = timestampParserMap.get(jsonColumn.getName());
                pageBuilder.setTimestamp(jsonColumn, parser.parse(value));
            }
        }
    }

    private static String getCurrentJoinBaseColumnValue(PageReader pageReader, Column joinBaseColumn)
    {
        if (pageReader.isNull(joinBaseColumn)) {
            return null;
        }

        if (Types.STRING.equals(joinBaseColumn.getType())) {
            return pageReader.getString(joinBaseColumn);
        }
        else if (Types.BOOLEAN.equals(joinBaseColumn.getType())) {
            return String.valueOf(pageReader.getBoolean(joinBaseColumn));
        }
        else if (Types.DOUBLE.equals(joinBaseColumn.getType())) {
            return String.valueOf(pageReader.getDouble(joinBaseColumn));
        }
        else if (Types.LONG.equals(joinBaseColumn.getType())) {
            return String.valueOf(pageReader.getLong(joinBaseColumn));
        }
        else if (Types.TIMESTAMP.equals(joinBaseColumn.getType())) {
            return String.valueOf(pageReader.getTimestamp(joinBaseColumn));
        }

        throw Throwables.propagate(new Throwable("Unsupported Column Type: " + joinBaseColumn.getType()));
    }
}

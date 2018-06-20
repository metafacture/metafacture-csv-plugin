/*
 * Copyright 2018 Deutsche Nationalbibliothek
 *
 * Licensed under the Apache License, Version 2.0 the "License";
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.metafacture.csv;

import com.opencsv.CSVWriter;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import org.metafacture.framework.FluxCommand;
import org.metafacture.framework.MetafactureException;
import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultStreamPipe;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A csv encoder that converts a record into a csv line (Default separator: comma).
 *
 * <P>
 *  Each record represents a row. Each literal value represents a column value.
 *  If a sequence of literals occur share the same name, a nested csv record is used as column value.
 * </P>
 */
@Description("Encodes each value in a record as a csv row.")
@In(StreamReceiver.class)
@Out(String.class)
@FluxCommand("encode-csv")
public class SimpleCsvEncoder extends DefaultStreamPipe<ObjectReceiver<String>> {

    private CSVWriter csvWriter;
    private StringWriter writer;

    /** List of items that will be written to a row */
    private List<String> rowItems;
    /** Last encountered literal name */
    private String lastLiteralName;
    /** List of literal values that has the same name */
    private List<String> literalValues;
    /** Flag for the first record encounter */
    private boolean isFirstRecord;
    /** Flag for the first literal encounter in a record */
    private boolean isFirstLiteral;

    private List<String> header;
    private char separator;
    private boolean includeHeader;
    private boolean includeRecordId;

    public SimpleCsvEncoder() {
        this(CSVWriter.DEFAULT_SEPARATOR);
    }

    public SimpleCsvEncoder(char separator) {
        this.separator = separator;
        this.includeRecordId = false;
        this.includeHeader = false;
        this.header = new ArrayList<>();

        this.isFirstRecord = true;
        this.isFirstLiteral = true;

        this.rowItems = new ArrayList<>();
        this.lastLiteralName = null;
        this.literalValues = new ArrayList<>();
    }

    /**
     * Start each line with the record id.
     */
    public void setIncludeRecordId(boolean includeRecordId) {
        this.includeRecordId = includeRecordId;
    }

    /**
     * Add a column description header.
     */
    public void setIncludeHeader(boolean includeHeader) {
        this.includeHeader = includeHeader;
    }

    public void setSeparator(String separator) {
        if (separator.length() > 1) {
            throw new MetafactureException("Separator needs to be a single character.");
        }
        this.separator = separator.charAt(0);
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    private void initialize() {
        writer = new StringWriter();
        String emptyLineEnd = "";
        csvWriter = new CSVWriter(writer,
                separator,
                CSVWriter.DEFAULT_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                emptyLineEnd);
    }

    private String[] arrayOf(List<String> list) {
        int length = list.size();
        return list.toArray(new String[length]);
    }

    private String innerRowOf(List<String> items) {
        StringWriter writer = new StringWriter();
        ICSVWriter csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(separator)
                .withQuoteChar(CSVWriter.DEFAULT_QUOTE_CHARACTER)
                .withLineEnd("")
                .build();

        String row[] = arrayOf(items);
        csvWriter.writeNext(row);
        String line = writer.toString().trim();
        return line;
    }

    private void resetCaches() {
        isFirstLiteral = true;
        literalValues = new ArrayList<>();
        rowItems = new ArrayList<>();
    }

    private void writeRow(List<String> rowItems) {
        String[] row = arrayOf(rowItems);
        csvWriter.writeNext(row);
        String line = writer.toString();
        getReceiver().process(line);

        writer.getBuffer().setLength(0);
    }

    @Override
    public void startRecord(final String identifier) {
        if (isFirstRecord) {
            initialize();
            if (includeRecordId) {
                header.add("record id");
            }
        }

        rowItems = new ArrayList<>();

        if (includeRecordId) {
            rowItems.add(identifier);
        }
    }

    @Override
    public void endRecord() {
        if (isFirstRecord) {
            if (includeHeader) {
                List<String> uniqueHeader = header.stream().distinct().collect(Collectors.toList());
                writeRow(uniqueHeader);
                header.clear();
            }
            isFirstRecord = false;
        }

        String rowItem = literalValues.size() == 1 ? literalValues.get(0) : innerRowOf(literalValues);
        rowItems.add(rowItem);

        writeRow(rowItems);

        resetCaches();
    }

    @Override
    public void literal(final String name, final String value) {
        if (isFirstRecord) {
            header.add(name);
        }

        if (isFirstLiteral) {
            lastLiteralName = name;
            isFirstLiteral = false;
        }

        if (name.equals(lastLiteralName)) {
            literalValues.add(value);
        } else {
            String rowItem = literalValues.size() == 1 ? literalValues.get(0) : innerRowOf(literalValues);
            rowItems.add(rowItem);

            literalValues = new ArrayList<>();
            literalValues.add(value);
        }

        lastLiteralName = name;
    }

    @Override
    public void onCloseStream() {
        try {
            csvWriter.close();
        } catch (IOException e) {
            throw new MetafactureException(e);
        }
    }

    @Override
    public void onResetStream() {
        this.includeRecordId = false;
        this.includeHeader = false;
        this.header = new ArrayList<>();

        this.isFirstRecord = true;
        this.isFirstLiteral = true;

        this.rowItems = new ArrayList<>();
        this.lastLiteralName = null;
        this.literalValues = new ArrayList<>();
    }

    @Override
    public void startEntity(final String name) {
        // Ignore
    }

    @Override
    public void endEntity() {
        // Ignore
    }
}

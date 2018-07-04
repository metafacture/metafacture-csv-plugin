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
    /** Flag for the first record encounter */
    private boolean isFirstRecord;

    private List<String> header;
    private char separator;
    private boolean noQuotes;
    private boolean includeHeader;
    private boolean includeRecordId;

    public SimpleCsvEncoder() {
        this(CSVWriter.DEFAULT_SEPARATOR);
    }

    public SimpleCsvEncoder(char separator) {
        this.separator = separator;
        this.noQuotes = false;
        this.includeHeader = false;
        this.includeRecordId = false;
        this.header = new ArrayList<>();

        this.isFirstRecord = true;
        this.rowItems = new ArrayList<>();
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

    public void setNoQuotes(boolean noQuotes) {
        this.noQuotes = noQuotes;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    private void initialize() {
        writer = new StringWriter();
        String emptyLineEnd = "";
        csvWriter = new CSVWriter(writer,
                separator,
                noQuotes ? CSVWriter.NO_QUOTE_CHARACTER : CSVWriter.DEFAULT_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                emptyLineEnd);
    }

    private String[] arrayOf(List<String> list) {
        int length = list.size();
        return list.toArray(new String[length]);
    }

    private void resetCaches() {
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

        writeRow(rowItems);

        resetCaches();
    }

    @Override
    public void literal(final String name, final String value) {
        if (isFirstRecord) {
            header.add(name);
        }
        rowItems.add(value);
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
        this.rowItems = new ArrayList<>();
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

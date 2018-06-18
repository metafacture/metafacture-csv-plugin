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

/**
 * A encoder that converts a record into a csv line (Default separator: comma).
 *
 * A csv line contains the value of each literal included in a record.
 */
@Description("Encodes each value in a record as a csv row.")
@In(StreamReceiver.class)
@Out(String.class)
@FluxCommand("encode-literal-csv")
public class CsvLiteralEncoder extends DefaultStreamPipe<ObjectReceiver<String>> {

    private StringWriter writer;
    private ICSVWriter csvWriter;
    private List<String> literalValueList;

    private char separator;
    private boolean onStart;
    private boolean includeRecordId;

    public CsvLiteralEncoder() {
        this(CSVWriter.DEFAULT_SEPARATOR);
    }

    public CsvLiteralEncoder(char separator) {
        this.separator = separator;
        this.onStart = true;
        this.includeRecordId = false;
    }

    /**
     * Start each line with the record id.
     */
    public void setIncludeRecordId(boolean includeRecordId) {
        this.includeRecordId = includeRecordId;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    private void initialize() {
        writer = new StringWriter();
        csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(separator)
                .withQuoteChar(CSVWriter.DEFAULT_QUOTE_CHARACTER)
                .withLineEnd("")
                .build();
    }

    private void resetStringWriter() {
        writer.getBuffer().setLength(0);
    }

    @Override
    public void startRecord(final String identifier) {
        if (onStart) {
            initialize();
            onStart = false;
        } else {
            resetStringWriter();
        }

        literalValueList = new ArrayList<>();
        if (includeRecordId) {
            literalValueList.add(identifier);
        }
    }

    @Override
    public void endRecord() {
        String[] literalValueArray = new String[literalValueList.size()];
        literalValueArray = literalValueList.toArray(literalValueArray);
        csvWriter.writeNext(literalValueArray);

        try {
            csvWriter.flush();
        } catch (IOException e) {
            throw new MetafactureException(e);
        }

        getReceiver().process(writer.toString());
    }

    @Override
    public void startEntity(final String name) {
        // Ignore
    }

    @Override
    public void endEntity() {
        // Ignore
    }

    @Override
    public void literal(final String name, final String value) {
        literalValueList.add(value);
    }

    @Override
    public void onCloseStream() {
        try {
            writer.close();
            csvWriter.close();
        } catch (IOException e) {
            throw new MetafactureException(e);
        }
    }
}

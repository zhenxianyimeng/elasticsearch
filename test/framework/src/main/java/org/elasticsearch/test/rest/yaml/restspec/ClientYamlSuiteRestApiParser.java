/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Parser for a {@link ClientYamlSuiteRestApi}.
 */
public class ClientYamlSuiteRestApiParser {

    private static final ObjectParser<Parameter, Void> PARAMETER_PARSER = new ObjectParser<>("parameter", true, Parameter::new);
    static {
        PARAMETER_PARSER.declareBoolean(Parameter::setRequired, new ParseField("required"));
    }

    public ClientYamlSuiteRestApi parse(String location, XContentParser parser) throws IOException {

        while ( parser.nextToken() != XContentParser.Token.FIELD_NAME ) {
            //move to first field name
        }

        String apiName = parser.currentName();
        if (location.endsWith(apiName + ".json") == false) {
            throw new IllegalArgumentException("API [" + apiName + "] should have the same name as its file [" + location + "]");
        }

        ClientYamlSuiteRestApi restApi = new ClientYamlSuiteRestApi(location, apiName);

        int level = -1;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT || level >= 0) {

            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {

                if ("url".equals(parser.currentName())) {
                    String currentFieldName = "url";
                    while(parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        }
                        if (parser.currentToken() == XContentParser.Token.START_ARRAY && "paths".equals(currentFieldName)) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                String path = null;
                                Set<String> methods = new HashSet<>();
                                Set<String> pathParts = new HashSet<>();
                                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                    if ("path".equals(parser.currentName())) {
                                        parser.nextToken();
                                        path = parser.text();
                                    }

                                    if ("methods".equals(parser.currentName())) {
                                        if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                                            throw new IllegalArgumentException(
                                                "Expected methods field in rest api definition to hold an array");
                                        }
                                        while (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
                                            String method = parser.text();
                                            if (methods.add(method) == false) {
                                                throw new IllegalArgumentException("Found duplicate method [" + method + "]");
                                            }
                                        }
                                    }

                                    if (parser.currentToken() == XContentParser.Token.START_OBJECT
                                        && "parts".equals(parser.currentName())) {
                                        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                                            String part = parser.currentName();
                                            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                                throw new IllegalArgumentException(
                                                    "Expected parts field in rest api definition to contain an object");
                                            }
                                            parser.skipChildren();
                                            if (pathParts.add(part) == false) {
                                                throw new IllegalArgumentException("duplicated path part [" + part + "]");
                                            }
                                        }
                                    }

                                    if (parser.currentToken() == XContentParser.Token.START_OBJECT
                                        && "deprecated".equals(parser.currentName())) {
                                        parser.skipChildren();
                                    }
                                }
                                restApi.addPath(path, methods.toArray(new String[0]), pathParts);
                            }
                            if (restApi.getPaths().isEmpty()) {
                                throw new IllegalArgumentException("At least one path should be listed under [paths]");
                            }
                        }
                    }
                }

                if ("params".equals(parser.currentName())) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new IllegalArgumentException("Expected params field in rest api definition to contain objects");

                    }
                    while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        String param = parser.currentName();
                        parser.nextToken();
                        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                            throw new IllegalArgumentException("Expected params field in rest api definition to contain an object");
                        }
                        restApi.addParam(param, PARAMETER_PARSER.parse(parser, null).isRequired());
                    }
                }

                if ("body".equals(parser.currentName())) {
                    parser.nextToken();
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        boolean requiredFound = false;
                        while(parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                                if ("required".equals(parser.currentName())) {
                                    requiredFound = true;
                                    parser.nextToken();
                                    if (parser.booleanValue()) {
                                        restApi.setBodyRequired();
                                    } else {
                                        restApi.setBodyOptional();
                                    }
                                }
                            }
                        }
                        if (false == requiredFound) {
                            restApi.setBodyOptional();
                        }
                    }
                }
            }

            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                level++;
            }
            if (parser.currentToken() == XContentParser.Token.END_OBJECT) {
                level--;
            }

        }

        parser.nextToken();
        assert parser.currentToken() == XContentParser.Token.END_OBJECT : "Expected [END_OBJECT] but was ["  + parser.currentToken() +"]";
        parser.nextToken();

        return restApi;
    }

    private static class Parameter {
        private boolean required;
        public boolean isRequired() {
            return required;
        }
        public void setRequired(boolean required) {
            this.required = required;
        }
    }
}

package com.zhuanzhuan.lineage.metadata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public final class DataMapMetadataClient {
    private static final String SEARCH_FILTER = "%257B%2522query%2522%253A%257B%2522bool%2522%253A%257B%257D%257D%257D";

    private final String baseUrl;
    private final String cookie;

    public DataMapMetadataClient(String baseUrl, String cookie) {
        this.baseUrl = trimTrailingSlash(baseUrl);
        this.cookie = cookie == null ? "" : cookie.trim();
    }

    public long findTableId(String fullTableName) throws IOException {
        ensureCookie();
        String encodedName = urlEncode(fullTableName);
        String url = baseUrl
                + "/api/data-map/search/query?q=" + encodedName
                + "&entity_type=table&from=0&size=10&query_filter=" + SEARCH_FILTER;
        JSONObject response = getJson(url);
        JSONObject data = response == null ? null : response.getJSONObject("data");
        JSONObject hitsObject = data == null ? null : data.getJSONObject("hits");
        JSONArray hits = hitsObject == null ? null : hitsObject.getJSONArray("hits");
        if (hits == null || hits.isEmpty()) {
            throw new IOException("DataMap search returned no table hits for " + fullTableName);
        }

        Long exactHiveId = null;
        Long exactTableId = null;
        Long fuzzyHiveId = null;
        Long fuzzyId = null;
        for (int i = 0; i < hits.size(); i++) {
            JSONObject hit = hits.getJSONObject(i);
            if (hit == null) {
                continue;
            }
            JSONObject source = hit.getJSONObject("_source");
            if (source == null) {
                continue;
            }
            Long tableId = source.getLong("id");
            if (tableId == null) {
                String rawId = hit.getString("_id");
                if (rawId != null && !rawId.trim().isEmpty()) {
                    try {
                        tableId = Long.parseLong(rawId.trim());
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
            if (tableId == null) {
                continue;
            }
            String displayName = source.getString("display_name");
            String serviceType = source.getString("service_type");
            String entityType = source.getString("entity_type");
            boolean hive = "Hive".equalsIgnoreCase(serviceType);
            boolean table = entityType == null || "table".equalsIgnoreCase(entityType);
            if (fullTableName.equals(displayName)) {
                if (hive && table) {
                    exactHiveId = tableId;
                    break;
                }
                if (table && exactTableId == null) {
                    exactTableId = tableId;
                }
                if (fuzzyId == null) {
                    fuzzyId = tableId;
                }
                continue;
            }
            if (hive && table && fuzzyHiveId == null) {
                fuzzyHiveId = tableId;
            }
            if (fuzzyId == null) {
                fuzzyId = tableId;
            }
        }

        if (exactHiveId != null) {
            return exactHiveId.longValue();
        }
        if (exactTableId != null) {
            return exactTableId.longValue();
        }
        if (fuzzyHiveId != null) {
            return fuzzyHiveId.longValue();
        }
        if (fuzzyId != null) {
            return fuzzyId.longValue();
        }
        throw new IOException("DataMap search did not return a usable table id for " + fullTableName);
    }

    public String fetchCreateSql(long tableId) throws IOException {
        ensureCookie();
        JSONObject response = getJson(baseUrl + "/api/data-map/table/create-sql?table_id=" + tableId);
        JSONObject data = response == null ? null : response.getJSONObject("data");
        String ddl = data == null ? null : data.getString("create_sql_hive");
        if (ddl == null || ddl.trim().isEmpty()) {
            throw new IOException("DataMap create-sql returned empty DDL for table_id=" + tableId);
        }
        return ddl.trim();
    }

    private JSONObject getJson(String url) throws IOException {
        String responseBody = runCurl(url);
        try {
            JSONObject json = parseJsonPayload(responseBody);
            if (json == null) {
                throw new IOException("DataMap returned empty JSON for url=" + url);
            }
            String status = json.getString("status");
            if (status != null && !"success".equalsIgnoreCase(status.trim())) {
                throw new IOException("DataMap returned non-success status for url=" + url + " | response=" + responseBody);
            }
            return json;
        } catch (Exception error) {
            if (error instanceof IOException) {
                throw (IOException) error;
            }
            throw new IOException("Failed to parse DataMap JSON response: " + responseBody, error);
        }
    }

    private String runCurl(String url) throws IOException {
        List<String> command = new ArrayList<String>();
        command.add("curl");
        command.add("--ipv4");
        command.add("--http1.1");
        command.add("--location");
        command.add("-sS");
        command.add("--retry");
        command.add("5");
        command.add("--retry-all-errors");
        command.add("--retry-max-time");
        command.add("90");
        command.add("--retry-delay");
        command.add("2");
        command.add("--connect-timeout");
        command.add("15");
        command.add("--max-time");
        command.add("30");
        command.add("--tlsv1.2");
        command.add(url);
        command.add("--header");
        command.add("cookie: " + cookie);

        Process process = null;
        try {
            process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start();
            String output = readAll(process.getInputStream());
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("DataMap curl failed with exitCode=" + exitCode + " for url=" + url + " | output=" + output);
            }
            return output;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while calling DataMap url=" + url, error);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
    }

    private String readAll(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = inputStream.read(buffer)) >= 0) {
            outputStream.write(buffer, 0, read);
        }
        return outputStream.toString("UTF-8");
    }

    private JSONObject parseJsonPayload(String responseBody) {
        if (responseBody == null) {
            return null;
        }
        try {
            return JSON.parseObject(responseBody);
        } catch (Exception ignored) {
        }
        int jsonStart = responseBody.indexOf("{\"status\"");
        int jsonEnd = responseBody.lastIndexOf('}');
        if (jsonStart >= 0 && jsonEnd > jsonStart) {
            String candidate = responseBody.substring(jsonStart, jsonEnd + 1);
            try {
                return JSON.parseObject(candidate);
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    private void ensureCookie() throws IOException {
        if (cookie.isEmpty()) {
            throw new IOException("DataMap cookie is required. Set zz.lineage.datamap.cookie or ZZ_LINEAGE_DATAMAP_COOKIE.");
        }
    }

    private String urlEncode(String value) throws IOException {
        return URLEncoder.encode(value, "UTF-8");
    }

    private String trimTrailingSlash(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}

package com.github.raphcal.notela.painless;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import fr.bdf.center.graalod.api.elastic.mock.SearchResponseHit;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 * @author Raphaël Calabro (raphael.calabro.external2@banque-france.fr)
 */
public abstract class Painless {

    public static Painless parse(final String script, final Gson gson) {
        if (script.matches("([a-zA-Z_][a-zA-Z0-9_.]*) = ([a-zA-Z_][a-zA-Z0-9_.]*)")) {
            return new Painless(gson) {
                @Override
                public void execute(JsonObject values) {
                    final String[] parts = script.split(" *= *");
                    setValue(values, parts[0], getValue(values, parts[1]));
                }
            };
        }
        throw new UnsupportedOperationException("Given Painless script is unsupported: " + script);
    }

    private static JsonElement getValue(JsonObject values, String field) {
        final String[] parts = field.split(Pattern.quote("."));
        JsonObject current = values;
        for (int index = 0; index < parts.length - 1; index++) {
            final JsonElement element = current.get(parts[index]);
            if (element != null && element.isJsonObject()) {
                current = element.getAsJsonObject();
            } else {
                return null;
            }
        }
        return current.get(parts[parts.length - 1]);
    }

    private static void setValue(JsonObject values, String field, JsonElement value) {
        final String[] parts = field.split(Pattern.quote("."));
        JsonObject current = values;
        for (int index = 0; index < parts.length - 1; index++) {
            final JsonElement element = current.get(parts[index]);
            if (element != null && element.isJsonObject()) {
                current = element.getAsJsonObject();
            } else {
                return;
            }
        }
        current.add(parts[parts.length - 1], value);
    }

    private final Gson gson;

    private Painless(Gson gson) {
        this.gson = gson;
    }

    public void execute(SearchResponseHit<JsonObject> document, Map<String, Object> parameters) {
        final HashMap<String, Object> map = new HashMap<>();
        map.put("ctx", document);
        map.put("params", parameters);
        final JsonObject object = gson.toJsonTree(map).getAsJsonObject();
        execute(object);
        document.setSource(object.get("ctx").getAsJsonObject()
                .get("_source").getAsJsonObject());
    }

    public abstract void execute(JsonObject values);

}

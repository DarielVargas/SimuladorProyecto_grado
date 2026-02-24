package com.dv.agroiot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Suscriptor {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Db db = new Db();

    // Nombres bonitos para los tipos 1..9
    private static final Map<Integer, String> TIPO_NOMBRE = new LinkedHashMap<>();
    static {
        TIPO_NOMBRE.put(1, "Humedad Suelo (%)");
        TIPO_NOMBRE.put(2, "Temp Ambiental (°C)");
        TIPO_NOMBRE.put(3, "Humedad Ambiental (%)");
        TIPO_NOMBRE.put(4, "Luz (Lux)");
        TIPO_NOMBRE.put(5, "pH Suelo");
        TIPO_NOMBRE.put(6, "Conductividad (dS/m)");
        TIPO_NOMBRE.put(7, "N (mg/kg)");
        TIPO_NOMBRE.put(8, "P (mg/kg)");
        TIPO_NOMBRE.put(9, "K (mg/kg)");
    }

    public void start() throws Exception {

        db.connect();

        // ✅ Escucha todas las estaciones que publiquen bajo /20181853/<EST-xxx>/mediciones
        String topic = Config.TOPIC_BASE + "/+/mediciones";

        MqttClient client = new MqttClient(
                Config.MQTT_BROKER,
                MqttClient.generateClientId()
        );

        MqttConnectOptions opt = new MqttConnectOptions();
        opt.setAutomaticReconnect(true);
        opt.setCleanSession(true);

        // ✅ Credenciales del profesor
        if (Config.MQTT_USER != null && !Config.MQTT_USER.isBlank()) {
            opt.setUserName(Config.MQTT_USER);
            opt.setPassword(Config.MQTT_PASS.toCharArray());
        }

        client.connect(opt);
        System.out.println("Suscriptor conectado a MQTT");
        System.out.println("Escuchando topic: " + topic);

        client.subscribe(topic, (t, msg) -> {
            String payload = new String(msg.getPayload(), StandardCharsets.UTF_8);

            try {
                JsonNode root = mapper.readTree(payload);

                // ✅ Validaciones para evitar NullPointer
                JsonNode estacionNode = root.get("estacion");
                JsonNode fechaNode = root.get("fecha");
                JsonNode mNode = root.get("m");

                if (estacionNode == null || estacionNode.asText().isBlank()) {
                    System.out.println("[WARN] Mensaje sin 'estacion': " + payload);
                    return;
                }
                if (fechaNode == null || fechaNode.asText().isBlank()) {
                    System.out.println("[WARN] Mensaje sin 'fecha': " + payload);
                    return;
                }
                if (mNode == null || !mNode.isArray()) {
                    System.out.println("[WARN] Mensaje sin array 'm': " + payload);
                    return;
                }

                String estacionCodigo = estacionNode.asText();
                String fechaStr = fechaNode.asText();
                LocalDateTime fecha = Db.parseFecha(fechaStr);

                // "m": [ {"t":1,"v":55.2}, ... ]
                Map<Integer, Double> tipoToValor = new HashMap<>();

                for (JsonNode item : mNode) {
                    JsonNode tNode = item.get("t");
                    JsonNode vNode = item.get("v");
                    if (tNode == null || vNode == null) continue;

                    int tipo = tNode.asInt();
                    double valor = vNode.asDouble();

                    tipoToValor.put(tipo, valor);
                }

                if (tipoToValor.isEmpty()) {
                    System.out.println("[WARN] Array m vacío o inválido: " + payload);
                    return;
                }

                // ✅ Inserta en BD
                db.insertarBatch(tipoToValor, estacionCodigo, fecha);

                // ✅ Impresión bonita en consola (lista)
                System.out.println("\n====================================================");
                System.out.println("ESTACION: " + estacionCodigo);
                System.out.println("TOPIC   : " + t);
                System.out.println("FECHA   : " + fechaStr);
                System.out.println();

                // ✅ Imprime primero los tipos 1..9 en orden, con (T#)
                for (Map.Entry<Integer, String> e : TIPO_NOMBRE.entrySet()) {
                    int tipo = e.getKey();
                    String nombre = e.getValue();

                    if (tipoToValor.containsKey(tipo)) {
                        System.out.printf("%-22s (T%-2d) : %.2f%n",
                                nombre,
                                tipo,
                                tipoToValor.get(tipo)
                        );
                    }
                }

                // Si llega algún tipo extra (no esperado), lo imprime al final
                for (Map.Entry<Integer, Double> e : tipoToValor.entrySet()) {
                    int tipo = e.getKey();
                    if (!TIPO_NOMBRE.containsKey(tipo)) {
                        System.out.printf("%-22s (T%-2d) : %.2f%n",
                                "Tipo Desconocido",
                                tipo,
                                e.getValue()
                        );
                    }
                }

                System.out.println("====================================================");

            } catch (Exception ex) {
                System.out.println("[DB ERROR] " + ex.getMessage());
                System.out.println("Topic: " + t);
                System.out.println("Payload: " + payload);
                ex.printStackTrace();
            }
        });
    }
}
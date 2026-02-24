package com.dv.agroiot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class Suscriptor {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Db db = new Db();

    public void start() throws Exception {

        db.connect();

        // ✅ Escucha todas las estaciones que publiquen bajo /20181853/<EST-xxx>/mediciones
        // Ej: /20181853/EST-001/mediciones
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

                // ✅ Ahora Db se encarga de:
                // - crear estación si no existe
                // - crear sensores si no existen
                // - insertar mediciones en batch
                db.insertarBatch(tipoToValor, estacionCodigo, fecha);

                System.out.println("[DB OK] topic=" + t + " estacion=" + estacionCodigo + " filas=" + tipoToValor.size());

            } catch (Exception ex) {
                System.out.println("[DB ERROR] " + ex.getMessage());
                System.out.println("Topic: " + t);
                System.out.println("Payload: " + payload);
                ex.printStackTrace();
            }
        });
    }
}
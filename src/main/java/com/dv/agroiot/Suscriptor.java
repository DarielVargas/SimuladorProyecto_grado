package com.dv.agroiot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class Suscriptor {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Db db = new Db();

    public void start() throws Exception {

        db.connect();

        //  UN SOLO TOPIC (igual que el publisher)
        String topic = Config.TOPIC_BASE + "/mediciones";

        MqttClient client = new MqttClient(
                Config.MQTT_BROKER,
                MqttClient.generateClientId()
        );

        MqttConnectOptions opt = new MqttConnectOptions();
        opt.setAutomaticReconnect(true);
        opt.setCleanSession(true);

        if (Config.MQTT_USER != null && !Config.MQTT_USER.isBlank()) {
            opt.setUserName(Config.MQTT_USER);
            opt.setPassword(Config.MQTT_PASS.toCharArray());
        }

        client.connect(opt);
        System.out.println("Suscriptor conectado a MQTT");
        System.out.println("Escuchando topic: " + topic);

        client.subscribe(topic, (t, msg) -> {
            String payload = new String(msg.getPayload());

            try {
                JsonNode root = mapper.readTree(payload);

                String estacionCodigo = root.get("estacion").asText();
                String fechaStr = root.get("fecha").asText();
                LocalDateTime fecha = Db.parseFecha(fechaStr);

                int estacionId = db.getEstacionIdPorCodigo(estacionCodigo);

                // "m": [ {"t":1,"v":55.2}, ... ]
                JsonNode m = root.get("m");
                Map<Integer, Double> tipoToValor = new HashMap<>();

                if (m != null && m.isArray()) {
                    for (JsonNode item : m) {
                        int tipo = item.get("t").asInt();
                        double valor = item.get("v").asDouble();
                        tipoToValor.put(tipo, valor);
                    }
                } else {
                    System.out.println("[WARN] Mensaje sin array m: " + payload);
                    return;
                }

                db.insertarBatch(tipoToValor, estacionId, fecha);
                System.out.println("[DB OK] estacion=" + estacionCodigo + " filas=" + tipoToValor.size());

            } catch (Exception ex) {
                System.out.println("[DB ERROR] " + ex.getMessage());
                System.out.println("Payload: " + payload);
            }
        });
    }
}
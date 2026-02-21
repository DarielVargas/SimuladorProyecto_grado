package com.dv.agroiot;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class Db {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Cache para no consultar sensores en cada mensaje
    // key: estacionId + ":" + tipoSensorId  -> sensorId
    private final Map<String, Integer> sensorCache = new HashMap<>();

    private Connection conn;

    public void connect() throws SQLException {
        conn = DriverManager.getConnection(Config.DB_URL, Config.DB_USER, Config.DB_PASS);
        conn.setAutoCommit(true);
        System.out.println("Conectado a MariaDB: " + Config.DB_URL);
    }

    public int getEstacionIdPorCodigo(String codigo) throws SQLException {
        String sql = "SELECT id FROM estaciones WHERE codigo = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, codigo);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        throw new SQLException("No existe estación con codigo=" + codigo);
    }

    public int getSensorId(int estacionId, int tipoSensorId) throws SQLException {
        String key = estacionId + ":" + tipoSensorId;
        Integer cached = sensorCache.get(key);
        if (cached != null) return cached;

        String sql = "SELECT id FROM sensores WHERE estacion_id = ? AND id_tipo_sensor = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, estacionId);
            ps.setInt(2, tipoSensorId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int sensorId = rs.getInt(1);
                    sensorCache.put(key, sensorId);
                    return sensorId;
                }
            }
        }
        throw new SQLException("No existe sensor para estacion_id=" + estacionId + " tipo=" + tipoSensorId);
    }

    public void insertarMedicion(int sensorId, double valor, LocalDateTime fecha) throws SQLException {
        String sql = "INSERT INTO mediciones (sensor_id, valor, fecha_medicion) VALUES (?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, sensorId);
            ps.setBigDecimal(2, new java.math.BigDecimal(String.valueOf(valor)));
            ps.setTimestamp(3, Timestamp.valueOf(fecha));
            ps.executeUpdate();
        }
    }

    public void insertarBatch(Map<Integer, Double> tipoToValor, int estacionId, LocalDateTime fecha) throws SQLException {
        String sql = "INSERT INTO mediciones (sensor_id, valor, fecha_medicion) VALUES (?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {

            for (Map.Entry<Integer, Double> e : tipoToValor.entrySet()) {
                int tipo = e.getKey();
                double valor = e.getValue();

                int sensorId = getSensorId(estacionId, tipo);

                ps.setInt(1, sensorId);
                ps.setBigDecimal(2, new java.math.BigDecimal(String.valueOf(valor)));
                ps.setTimestamp(3, Timestamp.valueOf(fecha));
                ps.addBatch();
            }

            ps.executeBatch();
        }
    }

    public static LocalDateTime parseFecha(String s) {
        return LocalDateTime.parse(s, FMT);
    }
}
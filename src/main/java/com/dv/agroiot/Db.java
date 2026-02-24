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

    /** Obtiene el id de la estación por código. Si no existe, la crea y devuelve el id. */
    public int getOrCreateEstacionId(String codigo) throws SQLException {
        Integer id = findEstacionId(codigo);
        if (id != null) return id;

        // Crear estación (ajusta columnas si tu tabla requiere más campos NOT NULL)
        String insert = "INSERT INTO estaciones (codigo) VALUES (?)";
        try (PreparedStatement ps = conn.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, codigo);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) {
                    int newId = keys.getInt(1);
                    System.out.println("[DB] Estación creada: codigo=" + codigo + " id=" + newId);
                    return newId;
                }
            }
        }

        // Fallback: re-consultar
        id = findEstacionId(codigo);
        if (id != null) return id;

        throw new SQLException("No se pudo crear/obtener estación con codigo=" + codigo);
    }

    private Integer findEstacionId(String codigo) throws SQLException {
        String sql = "SELECT id FROM estaciones WHERE codigo = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, codigo);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    /** Obtiene sensorId para (estacionId, tipoSensorId). Si no existe, lo crea y devuelve el id. */
    public int getOrCreateSensorId(int estacionId, int tipoSensorId) throws SQLException {
        String key = estacionId + ":" + tipoSensorId;
        Integer cached = sensorCache.get(key);
        if (cached != null) return cached;

        Integer id = findSensorId(estacionId, tipoSensorId);
        if (id != null) {
            sensorCache.put(key, id);
            return id;
        }

        // Crear sensor (ajusta columnas si tu tabla requiere más campos NOT NULL)
        String insert = "INSERT INTO sensores (estacion_id, id_tipo_sensor) VALUES (?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, estacionId);
            ps.setInt(2, tipoSensorId);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) {
                    int newId = keys.getInt(1);
                    sensorCache.put(key, newId);
                    System.out.println("[DB] Sensor creado: estacion_id=" + estacionId + " tipo=" + tipoSensorId + " id=" + newId);
                    return newId;
                }
            }
        }

        // Fallback: re-consultar
        id = findSensorId(estacionId, tipoSensorId);
        if (id != null) {
            sensorCache.put(key, id);
            return id;
        }

        throw new SQLException("No se pudo crear/obtener sensor para estacion_id=" + estacionId + " tipo=" + tipoSensorId);
    }

    private Integer findSensorId(int estacionId, int tipoSensorId) throws SQLException {
        String sql = "SELECT id FROM sensores WHERE estacion_id = ? AND id_tipo_sensor = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, estacionId);
            ps.setInt(2, tipoSensorId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
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

    public void insertarBatch(Map<Integer, Double> tipoToValor, String estacionCodigo, LocalDateTime fecha) throws SQLException {
        int estacionId = getOrCreateEstacionId(estacionCodigo);

        String sql = "INSERT INTO mediciones (sensor_id, valor, fecha_medicion) VALUES (?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {

            for (Map.Entry<Integer, Double> e : tipoToValor.entrySet()) {
                int tipo = e.getKey();
                double valor = e.getValue();

                int sensorId = getOrCreateSensorId(estacionId, tipo);

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
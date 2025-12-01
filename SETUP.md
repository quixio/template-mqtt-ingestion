# Setup Guide

This guide walks you through setting up the MQTT Simple Ingestion Pipeline template.

## Step 1: Configure Secrets

After syncing, you'll be prompted to configure the following secrets:

| Secret Key               | Description                                         | Used By                                    |
|--------------------------|-----------------------------------------------------|--------------------------------------------|
| `influxdb_admin_token`   | Admin token for InfluxDB authentication             | InfluxDB2, InfluxDB2 Sink, Grafana         |
| `influxdb_admin_password`| Admin password for InfluxDB                         | InfluxDB2                                  |
| `mqtt_password`          | Password for MQTT broker authentication             | MQTT Server, MQTT Source, MQTT Sink        |

> **WARNING**: These secrets protect services that may be publicly accessible.
> **Use strong, unique passwords for each secret.**

### Secret Configuration Tips

- **influxdb_admin_token**: Use a long, random string (e.g., 32+ characters). This token is used for API authentication.
- **influxdb_admin_password**: Standard password requirements apply. Used for InfluxDB admin UI access.
- **mqtt_password**: Used for authenticating MQTT clients to the broker. Keep this secure.

## Step 2: Verify Deployments

After syncing and configuring secrets, verify that all deployments are running:

1. Navigate to your Quix environment
2. Check the deployment status panel
3. All services should show as "Running" (green status)

**Expected running services:**

Core pipeline:
- Grafana
- MQTT Source
- MQTT Data Normalization
- InfluxDB2
- InfluxDB2 Sink
- MQTT Server

Example source (mock data generation):
- OPC UA Server
- OPC UA Source
- OPC UA to MQTT
- MQTT Sink

## Step 3: Access Services

### Grafana

1. Click the public URL link for the Grafana deployment
2. Login with:
   - **Username**: `admin`
   - **Password**: The value you set for `influxdb_admin_token`
3. Navigate to Dashboards to view the pre-configured dashboard

### InfluxDB2 (Optional)

InfluxDB2 is accessible via its internal service endpoint. If you need direct access:
- **Username**: `admin`
- **Password**: The value you set for `influxdb_admin_password`

## Step 4: Verify Data Flow

To confirm the pipeline is working:

1. Check Kafka topics for data:
   - `mqtt_data` - Raw MQTT data
   - `normalized_data` - Processed/aggregated data
   - `generated-opc_ua_data` - Data from OPC UA source (example source)
   - `external-mqtt_data` - Data being sent to MQTT sink (example source)

2. Open Grafana dashboard and verify data points are appearing

3. Data should flow within 1-2 minutes of all services starting

## Troubleshooting

### Services restarting repeatedly

This is normal during initial startup. Services may restart 3-5 times while dependencies initialize. Wait a few minutes for stabilization.

### No data in Grafana

1. Verify all services are running
2. Check the `normalized_data` topic has messages
3. Verify InfluxDB2 Sink logs for any connection errors
4. Ensure the InfluxDB token is correctly configured

### MQTT connection failures

1. Verify `mqtt_password` secret matches across all MQTT services
2. Check MQTT Server logs for authentication errors
3. Ensure MQTT Server is fully initialized before source/sink connect

### InfluxDB authentication errors

1. Verify `influxdb_admin_token` is set correctly
2. Check that the token is used consistently across InfluxDB2, InfluxDB2 Sink, and Grafana

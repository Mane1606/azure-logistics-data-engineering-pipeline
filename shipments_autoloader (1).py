{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "311c8049-35b4-4538-baf4-e8d792585580",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "dbutils.widgets.text(\"src\",\"\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e5fca672-77f3-4fd4-b9a7-eeed0cf9f3ce",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "execute_result",
     "data": {
      "text/plain": [
       "''"
      ]
     },
     "execution_count": 8,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "src_value = dbutils.widgets.get(\"src\")\n",
    "src_value"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "d0cbf6c4-28b0-4799-a2bf-b22f67266699",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "execute_result",
     "data": {
      "text/plain": [
       "<pyspark.sql.connect.streaming.query.StreamingQuery at 0xff61503e26f0>"
      ]
     },
     "execution_count": 24,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "from pyspark.sql.functions import current_timestamp, col\n",
    "\n",
    "input_path = f\"/Volumes/logistics_catalog/bronze/raw_volume/{src_value}\"\n",
    "checkpoint = f\"/Volumes/logistics_catalog/bronze/raw_volume/_checkpoints/{src_value}\"\n",
    "schema_path = f\"/Volumes/logistics_catalog/bronze/raw_volume/_schemas/{src_value}\"\n",
    "table_name = f\"logistics_catalog.bronze.{src_value}_bronze\"\n",
    "\n",
    "df = (\n",
    "    spark.readStream\n",
    "    .format(\"cloudFiles\")\n",
    "    .option(\"cloudFiles.format\", \"csv\")\n",
    "    .option(\"header\", \"true\")\n",
    "    .option(\"inferSchema\", \"true\")\n",
    "    .option(\"cloudFiles.schemaLocation\", schema_path)\n",
    "    .load(input_path)\n",
    "    .withColumn(\"ingestion_time\", current_timestamp())\n",
    "    .withColumn(\"source_file\", col(\"_metadata.file_path\"))  \n",
    ")\n",
    "\n",
    "(\n",
    "    df.writeStream\n",
    "    .format(\"delta\")\n",
    "    .option(\"checkpointLocation\", checkpoint)\n",
    "    .outputMode(\"append\")\n",
    "    .trigger(availableNow=True)   \n",
    "    .toTable(table_name)\n",
    ")\n",
    "\n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "4"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "mostRecentlyExecutedCommandWithImplicitDF": {
     "commandId": 4575622215668392,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "shipments_autoloader",
   "widgets": {
    "src": {
     "currentValue": "",
     "nuid": "2e63fcd8-2061-42df-9be0-1a856d704b7e",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "",
      "label": null,
      "name": "src",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String"
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "",
      "label": null,
      "name": "src",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    }
   }
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
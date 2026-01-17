import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import argparse
import csv


# =========================================================
# BIGQUERY SCHEMA (BRONZE)
# =========================================================
TABLE_SCHEMA = {
    "fields": [
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "show_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "duration_minutes", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "view_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}


# =========================================================
# PARSE CSV ROW
# =========================================================
class ParseCSVRow(beam.DoFn):
    def process(self, element):
        """
        CSV format:
        user_id,show_id,duration_minutes,device_type,view_date

        Example:
        u1,s1,30,mobile,2026-01-04
        """
        try:
            row = next(csv.reader([element]))

            yield {
                "user_id": row[0],
                "show_id": row[1],
                "duration_minutes": int(row[2]),
                "device_type": row[3],

                # 🔥 Convert DATE → TIMESTAMP
                "view_timestamp": f"{row[4]} 00:00:00",
            }

        except Exception:
            # Skip bad records safely
            return


# =========================================================
# MAIN PIPELINE
# =========================================================
def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="GCS path to input CSV file"
    )

    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery table: project:dataset.table"
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:

        (
            p
            | "Read CSV from GCS" >> beam.io.ReadFromText(
                known_args.input,
                skip_header_lines=1
            )

            | "Parse CSV Rows" >> beam.ParDo(ParseCSVRow())

            | "Write to BigQuery (Bronze)" >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=TABLE_SCHEMA,

                # ✅ REQUIRED
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    run()

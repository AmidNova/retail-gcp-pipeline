"""Run a Soda scan from an Airflow task (executed inside the soda venv)."""

import os

SODA_ROOT = "/opt/airflow/include/soda"
# Exposed to SodaCL as ${VAR}, for checks that query another dataset.
CHECK_VARIABLES = ("GCP_PROJECT_ID", "BQ_RAW_DATASET")


def check(scan_name: str, checks_subpath: str, data_source: str) -> dict:
    """Run the checks of one layer. Fails the task on failed checks or scan errors;
    warnings are logged and returned so the pipeline health table can count them."""
    from soda.scan import Scan

    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(f"{SODA_ROOT}/configuration.yml")
    scan.set_data_source_name(data_source)
    scan.add_variables({name: os.environ[name] for name in CHECK_VARIABLES})
    scan.add_sodacl_yaml_files(f"{SODA_ROOT}/checks/{checks_subpath}")
    scan.set_scan_definition_name(scan_name)

    exit_code = scan.execute()
    print(scan.get_logs_text())

    if scan.has_check_fails() or scan.has_error_logs():
        raise ValueError(f"Soda scan {scan_name} failed (exit code {exit_code})")
    return {
        "checks": len(scan.get_scan_results()["checks"]),
        # Soda Core 3.0 has no get_checks_warn(); fails are excluded above anyway.
        "warned": len(scan.get_checks_warn_or_fail()) - len(scan.get_checks_fail()),
    }

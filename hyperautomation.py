import subprocess
import json
import os
import glob
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import time
import traceback
import re
import ast

# --- Load external configuration from JSON file ---
CONFIG_FILE = "hyperautomation_config.json"
if not os.path.exists(CONFIG_FILE):
    print("FATAL ERROR: hyperautomation_config.json file is required but was not found.")
    exit(1)
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

# Configuration values
SERVICE_ACCOUNT_FILE = config["service_account_file"]
RESULTS_SPREADSHEET_ID = config["results_spreadsheet_id"]
RESULTS_WORKSHEET_NAME = config["results_worksheet_name"]  # Now loaded from config
CONFIG_SPREADSHEET_ID = config["config_spreadsheet_id"]
CONFIG_WORKSHEET_NAME = config["config_worksheet_name"]
HOST_USER_DATA_PATH = config["host_user_data_path"]
DOCKER_IMAGE = config["docker_image"]
DEFAULT_CONFIG_FILENAME = config["default_config_filename"]
FREQTRADE_USER_DATA_CONTAINER_PATH = config["freqtrade_user_data_container_path"]
HYPEROPT_RESULTS_DIR_HOST_PATH = os.path.join(HOST_USER_DATA_PATH, config["hyperopt_results_dir"])
HYPEROPT_SHOW_OUTPUT_FILE_HOST = os.path.join(HOST_USER_DATA_PATH, config["hyperopt_show_output_file"])
DEFAULT_LOSS_FUNCTION = config["default_loss_function"]
DEFAULT_JOB_WORKERS = config["default_job_workers"]
DEFAULT_TIMEFRAME_DETAIL = config.get("timeframe_detail", None)
SCOPES = config.get("scopes", ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"])

# --- Parse headers from config ---
headers_config = config["headers"]["config"]
headers_results = config["headers"]["results"]
headers_strategy = config["headers"]["strategy"]

CONFIG_HEADERS = headers_config["fields"] if headers_config.get("use_config_headers", False) else []
RESULTS_HEADERS = headers_results["fields"] if headers_results.get("use_results_headers", False) else []
STRATEGY_HEADERS = headers_strategy.get("fields", [])

# Combine all headers into RESULT_HEADERS
RESULT_HEADERS = CONFIG_HEADERS + STRATEGY_HEADERS + RESULTS_HEADERS
if not RESULT_HEADERS:
    print("FATAL ERROR: No headers defined after processing config.")
    exit(1)

def get_value_from_dict(data_dict, key, default=""):
    val = data_dict.get(key)
    if val in [None, "", "#N/A"]:
        return default
    return val

def get_numeric_value(data_dict, key, default="N/A"):
    val = data_dict.get(key)
    if val in [None, "", "#N/A"]:
        return default
    try:
        s = str(val).strip()
        if not s:
            return default
        return float(s) if "." in s else int(s)
    except (ValueError, TypeError):
        return str(val)

def parse_duration(duration_str):
    try:
        parts = list(map(int, duration_str.split(":")))
        if len(parts) == 3:
            h, m, s = parts
            return round(h * 60 + m + s / 60)
        elif len(parts) == 2:
            m, s = parts
            return round(m + s / 60)
    except Exception:
        return "N/A"

def authenticate_gsheet():
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        print("GS Auth OK.")
        return client
    except Exception as e:
        print(f"ERROR: GS Auth failed: {e}")
        return None

def get_worksheet(client, spreadsheet_id, worksheet_name):
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        print(f"Accessed sheet: '{worksheet_name}'")
        return worksheet
    except Exception as e:
        print(f"ERROR: Cannot open worksheet '{worksheet_name}': {e}")
        return None

def read_hyperopt_runs_from_sheet(config_worksheet):
    print(f"Reading runs from sheet '{config_worksheet.title}'...")
    expected_headers = ["Runs", "Config", "Strategy", "Pairs", "Leverage", "% per trade", "epochs", "spaces", "timerange", "loss_function", "jobs", "min_trades", "random_state", "timeframe_detail"]
    try:
        all_runs_data = config_worksheet.get_all_records(head=1)
        valid_runs = []
        required_columns = ["epochs", "timerange", "Strategy"]
        if not all_runs_data and config_worksheet.row_count > 0:
            print("Warning: Sheet has rows but get_all_records returned empty. Check header row.")
            return []
        print(f"Processing {len(all_runs_data)} potential runs...")
        for i, run_data in enumerate(all_runs_data):
            row_num = i + 2
            if any(get_value_from_dict(run_data, col) == "" for col in required_columns):
                print(f"Skipping Row {row_num}: A required column is empty.")
                continue
            run_dict = {
                "strategy_name": str(get_value_from_dict(run_data, "Strategy")),
                "config_filename": str(get_value_from_dict(run_data, "Config")),
                "epochs": str(get_value_from_dict(run_data, "epochs")),
                "timerange": str(get_value_from_dict(run_data, "timerange")),
                "Leverage": str(get_value_from_dict(run_data, "Leverage")),
                "% per trade": str(get_value_from_dict(run_data, "% per trade")),
                "Pairs": str(get_value_from_dict(run_data, "Pairs")),
            }
            for key in ["spaces", "loss_function", "jobs", "min_trades", "random_state", "timeframe_detail"]:
                value = get_value_from_dict(run_data, key)
                if value != "" and value != "OFF":
                    run_dict[key] = str(value)
            valid_runs.append(run_dict)
        if not valid_runs:
            print("Warning: No valid runs found after processing sheet.")
        else:
            print(f"Prepared {len(valid_runs)} valid runs.")
        return valid_runs
    except Exception as e:
        print(f"ERROR: Failed reading config sheet: {e}")
        traceback.print_exc()
        return None

def run_hyperopt_docker(run_params):
    strategy_to_run = run_params["strategy_name"]
    config_filename = run_params["config_filename"]
    config_path_in_container = f"{FREQTRADE_USER_DATA_CONTAINER_PATH.rstrip('/')}/{config_filename.lstrip('/')}"
    loss_function = run_params.get("loss_function", DEFAULT_LOSS_FUNCTION)
    input_random_state = run_params.get("random_state")
    timeframe_detail = run_params.get("timeframe_detail", DEFAULT_TIMEFRAME_DETAIL)
    should_capture_random_state = input_random_state is None
    docker_command = [
        "docker", "run", "-it", "--rm",
        "-v", f"{HOST_USER_DATA_PATH}:{FREQTRADE_USER_DATA_CONTAINER_PATH}",
        DOCKER_IMAGE, "hyperopt",
        "--config", config_path_in_container,
        "--strategy", strategy_to_run,
        "--hyperopt-loss", loss_function,
        "--epochs", run_params["epochs"],
        "--timerange", run_params["timerange"]
    ]
    if "spaces" in run_params:
        docker_command.extend(["--spaces", run_params["spaces"]])
    if "jobs" in run_params:
        docker_command.extend(["-j", run_params["jobs"]])
    else:
        docker_command.extend(["-j", str(DEFAULT_JOB_WORKERS)])
    if "min_trades" in run_params:
        docker_command.extend(["--min-trades", run_params["min_trades"]])
    if input_random_state is not None:
        docker_command.extend(["--random-state", input_random_state])
    if timeframe_detail and timeframe_detail.strip():
        docker_command.extend(["--timeframe-detail", timeframe_detail])
    print(f"\n--- Running Docker Hyperopt: {' '.join(docker_command)} ---")
    print("--- Freqtrade output streams below (Live) ---")
    captured_random_state = None
    buffer = ""
    ansi_escape = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
    try:
        process = subprocess.Popen(docker_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, encoding="utf-8", errors="replace", bufsize=1)
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            if line:
                print(line, end="")
                clean_line = ansi_escape.sub("", line)
                buffer += clean_line
                if should_capture_random_state and captured_random_state is None:
                    rs_match = re.search(r'optimizer random state:\s*(\d+)', buffer, re.IGNORECASE)
                    if rs_match:
                        captured_random_state = rs_match.group(1)
                        print(f"\n--- Captured Random State: {captured_random_state} ---")
                        buffer = ""
        return_code = process.wait()
        reported_random_state = captured_random_state if captured_random_state is not None else input_random_state
        if return_code == 0:
            print(f"\n--- Freqtrade process completed successfully (RC: {return_code}) ---")
            return True, reported_random_state
        else:
            print(f"\nERROR: Docker Freqtrade command failed (RC:{return_code}).")
            return False, reported_random_state
    except Exception as e:
        print(f"ERROR: Unexpected error running/reading Docker: {e}")
        traceback.print_exc()
        return False, input_random_state

def run_hyperopt_show_docker(config_filename, results_filename_host, strategy_name):
    config_path_in_container = f"{FREQTRADE_USER_DATA_CONTAINER_PATH.rstrip('/')}/{config_filename.lstrip('/')}"
    results_basename = os.path.basename(results_filename_host)
    docker_command = [
        "docker", "run", "--rm",
        "-v", f"{HOST_USER_DATA_PATH}:{FREQTRADE_USER_DATA_CONTAINER_PATH}",
        DOCKER_IMAGE, "hyperopt-show",
        "--config", config_path_in_container,
        "--hyperopt-filename", results_basename,
        "--best", "-n", "1", "--no-color"
    ]
    print(f"\n--- Running Docker hyperopt-show: {' '.join(docker_command)} ---")
    try:
        process = subprocess.run(docker_command, capture_output=True, text=True, check=True,
                                   encoding="utf-8", errors="replace")
        print("--- hyperopt-show process completed successfully ---")
        return process.stdout
    except Exception as e:
        print(f"ERROR: hyperopt-show command failed: {e}")
        traceback.print_exc()
        return None

def find_latest_hyperopt_result_file(results_dir_host, strategy_name):
    try:
        print("Waiting 5s for results file...")
        time.sleep(5)
        if not os.path.isdir(results_dir_host):
            print(f"ERROR: Results dir not found: '{results_dir_host}'.")
            return None
        search_pattern = os.path.join(results_dir_host, f"strategy_{strategy_name}*.fthypt")
        result_files = glob.glob(search_pattern)
        if not result_files:
            print(f"Warning: No results files found: '{search_pattern}'.")
            return None
        latest_file = max(result_files, key=os.path.getctime)
        print(f"Found results file: {os.path.basename(latest_file)}")
        return latest_file
    except Exception as e:
        print(f"Error finding results file: {e}")
        return None

def parse_hyperopt_show_output(show_output_content, run_params_for_context, run_index, reported_random_state):
    print("Parsing hyperopt-show output...")
    if not show_output_content:
        print("ERROR: No hyperopt-show output content provided.")
        return None
    lines = show_output_content.splitlines()
    buy_params = {}
    sell_params = {}
    metrics = {header: "N/A" for header in RESULTS_HEADERS}
    try:
        # Parse hyperspace params
        buy_section = []
        sell_section = []
        in_buy = False
        in_sell = False
        param_block_start_index = -1
        for idx, line in reversed(list(enumerate(lines))):
            if "# Buy hyperspace params:" in line.strip():
                param_block_start_index = idx
                break
        if param_block_start_index != -1:
            print("Found parameter block start marker.")
            for line in lines[param_block_start_index:]:
                stripped = line.strip()
                if "# Buy hyperspace params:" in stripped:
                    in_buy = True
                    in_sell = False
                    continue
                if "# Sell hyperspace params:" in stripped:
                    in_buy = False
                    in_sell = True
                    continue
                if stripped.startswith("# ROI table:") or stripped.startswith("# Stoploss:"):
                    in_buy = False
                    in_sell = False
                    continue
                if stripped.startswith("# Trailing stop:") or stripped.startswith("# Max Open Trades:"):
                    break
                if in_buy and stripped.startswith('"'):
                    buy_section.append(line)
                elif in_sell and stripped.startswith('"'):
                    sell_section.append(line)
            if buy_section:
                buy_dict_str = "{\n" + "\n".join(buy_section).strip().rstrip(",") + "\n}"
                try:
                    buy_params = ast.literal_eval(buy_dict_str)
                    print("Parsed buy_params.")
                except Exception as e:
                    print(f"Warning: Failed parsing buy_params: {e}")
            if sell_section:
                sell_dict_str = "{\n" + "\n".join(sell_section).strip().rstrip(",") + "\n}"
                try:
                    sell_params = ast.literal_eval(sell_dict_str)
                    print("Parsed sell_params.")
                except Exception as e:
                    print(f"Warning: Failed parsing sell_params: {e}")
        else:
            print("Warning: Could not find param block marker.")

        print("Parsing SUMMARY METRICS table...")
        in_summary = False
        summary_found = False
        for line in lines:
            stripped = line.strip()
            if "SUMMARY METRICS" in stripped:
                in_summary = True
                continue
            if not in_summary:
                continue
            if not stripped or len(stripped) < 5:
                continue
            if "│" in stripped:
                parts = [p.strip() for p in stripped.split("│") if p.strip()]
            else:
                parts = re.split(r'\s{2,}', stripped)
            if len(parts) >= 2:
                metric_name, metric_value = parts[0], parts[-1]
                summary_found = True
                if "Total/Daily Avg Trades" in metric_name and "Trades #" in RESULTS_HEADERS:
                    metrics["Trades #"] = metric_value.split("/")[0].strip()
                elif "Total profit %" in metric_name and "Profit %" in RESULTS_HEADERS:
                    metrics["Profit %"] = metric_value.replace("%", "").strip()
                elif "Absolute Drawdown (Account)" in metric_name and "DrawDown %" in RESULTS_HEADERS:
                    metrics["DrawDown %"] = metric_value.replace("%", "").strip()
                elif "Market change" in metric_name:
                    break
        if not summary_found and RESULTS_HEADERS:
            print("Warning: SUMMARY METRICS table not found/parsed.")

        print("Parsing BACKTESTING REPORT table for Win%/Avg Profit%...")
        total_row_found = False
        i_line = 0
        while i_line < len(lines):
            line = lines[i_line]
            if line.strip().startswith("│") and "TOTAL" in line:
                total_row_lines = [line]
                j = i_line + 1
                while j < len(lines) and lines[j].strip().startswith("│") and not lines[j].strip().startswith("│ TOTAL"):
                    total_row_lines.append(lines[j])
                    j += 1
                first_line = total_row_lines[0]
                fields = [p.strip() for p in first_line.split("│") if p.strip()]
                if len(total_row_lines) > 1:
                    last_line = total_row_lines[-1]
                    last_fields = [p.strip() for p in last_line.split("│") if p.strip()]
                    win_value = last_fields[-1] if last_fields else fields[-1]
                else:
                    win_value = fields[-1]
                if "Trades #" in RESULTS_HEADERS:
                    metrics["Trades #"] = fields[1]
                if "Avg. Profit %" in RESULTS_HEADERS:
                    metrics["Avg. Profit %"] = fields[2].replace("%", "")
                if "Profit %" in RESULTS_HEADERS:
                    metrics["Profit %"] = fields[4].replace("%", "")
                if "Duration min" in RESULTS_HEADERS:
                    metrics["Duration min"] = parse_duration(fields[5])
                if "% Win" in RESULTS_HEADERS:
                    metrics["% Win"] = win_value
                total_row_found = True
                break
            i_line += 1
        if not total_row_found and RESULTS_HEADERS:
            print("Warning: TOTAL row not found in BACKTESTING REPORT table.")

        parsed_data = {header: "N/A" for header in RESULT_HEADERS}
        if CONFIG_HEADERS:
            parsed_data.update({
                "Run #": str(run_index),
                "Strategy": get_value_from_dict(run_params_for_context, "strategy_name", "N/A"),
                "Config": get_value_from_dict(run_params_for_context, "config_filename", DEFAULT_CONFIG_FILENAME),
                "Epochs": get_value_from_dict(run_params_for_context, "epochs"),
                "random-state": reported_random_state if reported_random_state is not None else "N/A",
                "Timerange": get_value_from_dict(run_params_for_context, "timerange"),
                "Pairs": get_value_from_dict(run_params_for_context, "Pairs", "N/A"),
                "loss_function": get_value_from_dict(run_params_for_context, "loss_function", DEFAULT_LOSS_FUNCTION),
                "Leverage": get_value_from_dict(run_params_for_context, "Leverage", "N/A"),
                "% per trade": get_value_from_dict(run_params_for_context, "% per trade", "N/A"),
                "Date and Time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            })
        for param in STRATEGY_HEADERS:
            parsed_data[param] = buy_params.get(param, sell_params.get(param, "N/A"))
        if RESULTS_HEADERS:
            parsed_data.update(metrics)

        print("Successfully parsed hyperopt-show output.")
        return parsed_data
    except Exception as e:
        print(f"ERROR: Unexpected error during parsing hyperopt-show output: {e}")
        traceback.print_exc()
        return None

def find_next_empty_run_row(worksheet):
    try:
        all_rows = worksheet.get_all_values()
        return len(all_rows) + 1
    except Exception as e:
        print(f"ERROR: Could not determine the next empty row: {e}")
        return worksheet.row_count + 1

def write_results_to_row(worksheet, data_dict):
    try:
        target_row = find_next_empty_run_row(worksheet)
        print(f"Appending data to Row {target_row}.")
        current_rows = worksheet.row_count
        if target_row > current_rows:
            worksheet.resize(target_row, worksheet.col_count)
            print(f"Resized worksheet to {target_row} rows and {worksheet.col_count} columns.")
        header_row = worksheet.row_values(1)
        cell_list = []
        for header in RESULT_HEADERS:
            if header in header_row:
                col_index = header_row.index(header) + 1
                value = str(data_dict.get(header, ""))
                cell_list.append(gspread.Cell(target_row, col_index, value))
        if cell_list:
            worksheet.update_cells(cell_list, value_input_option="USER_ENTERED")
        print(f"Successfully wrote results to Row {target_row}.")
        return True
    except Exception as e:
        print(f"ERROR: Could not write data to row: {e}")
        traceback.print_exc()
        return False

def get_next_run_number(results_worksheet):
    try:
        header_row = results_worksheet.row_values(1)
        if "Run #" not in header_row:
            print("ERROR: 'Run #' header not found in results sheet.")
            return 1
        run_col_index = header_row.index("Run #") + 1
        run_numbers = []
        col_values = results_worksheet.col_values(run_col_index)[1:]
        for cell in col_values:
            try:
                run_numbers.append(int(cell))
            except:
                continue
        if run_numbers:
            return max(run_numbers) + 1
        else:
            return 1
    except Exception as e:
        print(f"ERROR: Unable to determine next run number: {e}")
        return 1

if __name__ == "__main__":
    script_start_time = time.time()
    print(f"Starting Script: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    gs_client = authenticate_gsheet()
    if not gs_client:
        exit(1)
    config_worksheet = get_worksheet(gs_client, CONFIG_SPREADSHEET_ID, CONFIG_WORKSHEET_NAME)
    if not config_worksheet:
        exit(1)
    results_worksheet = get_worksheet(gs_client, RESULTS_SPREADSHEET_ID, RESULTS_WORKSHEET_NAME)  # Uses config value
    if not results_worksheet:
        exit(1)
    
    hyperopt_runs = read_hyperopt_runs_from_sheet(config_worksheet)
    if hyperopt_runs is None or not hyperopt_runs:
        print("Exit: No valid runs in config.")
        exit(1)
    
    next_run_number = get_next_run_number(results_worksheet)
    print(f"Next run number will be: {next_run_number}")
    
    successful_runs_count = 0
    failed_runs_count = 0
    print(f"\n--- Starting Processing for {len(hyperopt_runs)} Runs ---")
    
    for i, run_params in enumerate(hyperopt_runs):
        current_run_number = next_run_number + i
        run_params["run_number"] = current_run_number
        print(f"\n======= RUN {current_run_number} | Strategy: {run_params['strategy_name']} =======")
        
        run_successful, reported_random_state = run_hyperopt_docker(run_params)
        parsed_result = None
        
        if run_successful:
            latest_file = find_latest_hyperopt_result_file(HYPEROPT_RESULTS_DIR_HOST_PATH, run_params["strategy_name"])
            if latest_file:
                hyperopt_show_stdout = run_hyperopt_show_docker(run_params["config_filename"], latest_file, run_params["strategy_name"])
                if hyperopt_show_stdout:
                    try:
                        with open(HYPEROPT_SHOW_OUTPUT_FILE_HOST, "w", encoding="utf-8") as f:
                            f.write(hyperopt_show_stdout)
                        print(f"Saved hyperopt-show output to: {HYPEROPT_SHOW_OUTPUT_FILE_HOST}")
                        parsed_result = parse_hyperopt_show_output(hyperopt_show_stdout, run_params, i, reported_random_state)
                        if parsed_result is not None:
                            parsed_result["Run #"] = str(current_run_number)
                        else:
                            print(f"Run {current_run_number} FAILED: Parsing hyperopt-show output failed.")
                    except Exception as e:
                        print(f"ERROR: Saving/Parsing hyperopt-show output: {e}")
                        traceback.print_exc()
                else:
                    print(f"Run {current_run_number} FAILED: hyperopt-show command failed.")
            else:
                print(f"Run {current_run_number} FAILED: Could not find .fthypt file.")
        else:
            print(f"Run {current_run_number} FAILED: Main hyperopt command error.")
        
        if parsed_result:
            if write_results_to_row(results_worksheet, parsed_result):
                successful_runs_count += 1
                print(f"Run {current_run_number} results appended to results sheet.")
            else:
                failed_runs_count += 1
        else:
            failed_runs_count += 1
            print("Attempting to write partial data...")
            partial_result = {header: "FAILED" for header in RESULT_HEADERS}
            if "Run #" in RESULT_HEADERS:
                partial_result["Run #"] = str(current_run_number)
            if "Strategy" in RESULT_HEADERS:
                partial_result["Strategy"] = get_value_from_dict(run_params, "strategy_name", "N/A")
            if "Config" in RESULT_HEADERS:
                partial_result["Config"] = get_value_from_dict(run_params, "config_filename")
            if "Epochs" in RESULT_HEADERS:
                partial_result["Epochs"] = get_value_from_dict(run_params, "epochs")
            if "random-state" in RESULT_HEADERS:
                partial_result["random-state"] = reported_random_state if reported_random_state is not None else "N/A"
            if "Timerange" in RESULT_HEADERS:
                partial_result["Timerange"] = get_value_from_dict(run_params, "timerange")
            if "Pairs" in RESULT_HEADERS:
                partial_result["Pairs"] = get_value_from_dict(run_params, "Pairs", "N/A")
            if "loss_function" in RESULT_HEADERS:
                partial_result["loss_function"] = get_value_from_dict(run_params, "loss_function", DEFAULT_LOSS_FUNCTION)
            if "Leverage" in RESULT_HEADERS:
                partial_result["Leverage"] = get_value_from_dict(run_params, "Leverage", "N/A")
            if "% per trade" in RESULT_HEADERS:
                partial_result["% per trade"] = get_value_from_dict(run_params, "% per trade", "N/A")
            if write_results_to_row(results_worksheet, partial_result):
                print(f"Run {current_run_number} partial data appended to results sheet.")
        
        print(f"======= End Run {current_run_number} =======")
    
    script_end_time = time.time()
    print("\n--- Script Finished ---")
    print(f"Completed at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Total execution time: {script_end_time - script_start_time:.2f} seconds")
    print(f"Successful Runs (Results Logged): {successful_runs_count}")
    print(f"Failed Runs (Execution or Logging): {failed_runs_count}")

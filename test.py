import pandas as pd
import os
import logging
import json
from openpyxl.utils.exceptions import InvalidFileException

def process_monthly_excel_file_with_sheet_tracking(
    excel_path,
    processed_registry_path='processed_files.json',
    processed_ids_dir='processed_ids',
    size_threshold_mb=10
):
    """
    Processes monthly Excel files with deduplication and per-sheet tracking.
    Tracks processed sheets per file to avoid reprocessing sheets on file re-runs.
    """

    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    EXPECTED_COLUMNS = {
        'orders': {'order_num', 'order_id', 'user_id', 'order_timestamp', 'total_amount', 'date'},
        'order_items': {'id', 'order_id', 'user_id', 'days_since_prior_order', 'product_id', 'add_to_cart_order', 'reordered', 'order_timestamp', 'date'},
        'products': {'product_id', 'department_id', 'department', 'product_name'}
    }

    UNIQUE_KEYS = {
        'orders': 'order_id',
        'order_items': 'id',
        'products': 'product_id'
    }

    os.makedirs(processed_ids_dir, exist_ok=True)

    def load_processed_registry():
        if os.path.exists(processed_registry_path):
            try:
                with open(processed_registry_path, 'r') as f:
                    return json.load(f)  # dict: {filename: [sheet1, sheet2, ...]}
            except Exception as e:
                logging.warning(f"Could not load processed registry file: {e}")
                return {}
        return {}

    def save_processed_registry(registry):
        try:
            with open(processed_registry_path, 'w') as f:
                json.dump(registry, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save processed registry: {e}")

    def load_processed_ids(data_type):
        path = os.path.join(processed_ids_dir, f'{data_type}_ids.json')
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    return set(json.load(f))
            except Exception as e:
                logging.warning(f"Could not load processed IDs for {data_type}: {e}")
                return set()
        return set()

    def save_processed_ids(data_type, ids_set):
        path = os.path.join(processed_ids_dir, f'{data_type}_ids.json')
        try:
            with open(path, 'w') as f:
                json.dump(list(ids_set), f)
        except Exception as e:
            logging.error(f"Failed to save processed IDs for {data_type}: {e}")

    def columns_match(expected_cols, df_cols):
        return expected_cols == set(df_cols)

    def identify_sheet_type(df):
        for data_type, cols in EXPECTED_COLUMNS.items():
            if columns_match(cols, df.columns):
                return data_type
        return None

    def append_to_csv(df, data_type):
        csv_file = f'{data_type}_combined.csv'
        header = not os.path.exists(csv_file)
        try:
            df.to_csv(csv_file, mode='a', index=False, header=header)
            logging.info(f"Appended {len(df)} new rows to {csv_file}")
        except Exception as e:
            logging.error(f"Failed to append data to {csv_file}: {e}")

    processed_registry = load_processed_registry()
    filename = os.path.basename(excel_path)

    # Get list of sheets already processed for this file (empty list if none)
    processed_sheets_for_file = set(processed_registry.get(filename, []))

    try:
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"File not found: {excel_path}")

        file_size_mb = os.path.getsize(excel_path) / (1024 * 1024)
        logging.info(f"Processing file '{excel_path}' ({file_size_mb:.2f} MB)")

        xls = pd.ExcelFile(excel_path, engine='openpyxl')
        sheet_names = xls.sheet_names

        if not sheet_names:
            raise ValueError("Excel file contains no sheets")

        # Load processed IDs for all data types once
        processed_ids = {dt: load_processed_ids(dt) for dt in EXPECTED_COLUMNS.keys()}

        # Filter sheets to process: only those NOT in processed_sheets_for_file
        sheets_to_process = [s for s in sheet_names if s not in processed_sheets_for_file]

        if not sheets_to_process:
            logging.info(f"All sheets in file '{filename}' have already been processed. Skipping.")
            return

        # Process sheets one by one (chunking) regardless of file size for simplicity
        for i, sheet in enumerate(sheets_to_process, start=1):
            logging.info(f"Processing sheet {i}/{len(sheets_to_process)}: '{sheet}'")
            try:
                df = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl')

                if df.empty:
                    logging.warning(f"Sheet '{sheet}' is empty, skipping.")
                    # Mark empty sheet as processed to avoid reprocessing
                    processed_sheets_for_file.add(sheet)
                    continue

                sheet_type = identify_sheet_type(df)

                if sheet_type:
                    key_col = UNIQUE_KEYS[sheet_type]
                    before_count = len(df)
                    df = df[~df[key_col].isin(processed_ids[sheet_type])]
                    after_count = len(df)

                    if after_count == 0:
                        logging.info(f"All rows in sheet '{sheet}' are duplicates, skipping append.")
                    else:
                        append_to_csv(df, sheet_type)
                        processed_ids[sheet_type].update(df[key_col].astype(str).tolist())
                        logging.info(f"Filtered {before_count - after_count} duplicate rows in sheet '{sheet}'")

                    # Mark sheet as processed
                    processed_sheets_for_file.add(sheet)

                else:
                    logging.warning(f"Sheet '{sheet}' columns do not match any expected schema. Ignoring.")
                    # Still mark as processed to avoid repeated attempts
                    processed_sheets_for_file.add(sheet)

            except Exception as e:
                logging.error(f"Error reading sheet '{sheet}': {e}")

        # Save updated processed IDs
        for dt, ids_set in processed_ids.items():
            save_processed_ids(dt, ids_set)

        # Update registry with new processed sheets for this file
        processed_registry[filename] = list(processed_sheets_for_file)
        save_processed_registry(processed_registry)

        logging.info(f"Finished processing file '{filename}'")

    except FileNotFoundError as e:
        logging.error(e)
    except PermissionError as e:
        logging.error(f"Permission denied: {e}")
    except InvalidFileException as e:
        logging.error(f"Invalid or corrupted Excel file: {e}")
    except ValueError as e:
        logging.error(f"Value error: {e}")
    except MemoryError:
        logging.error("Memory error: File too large to process.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")




process_monthly_excel_file_with_sheet_tracking(r'C:\Users\jason\OneDrive\Desktop\p2-fin\L5\LAB5_LAKEHOUSE_ARCHITECTURE_FOR_E-COMMERCE_TRANSACTIONS\data\raw\order_items_apr_2025.xlsx', size_threshold_mb=10)


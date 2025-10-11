import os
import csv
import logging
import gc
from collections import defaultdict
from shared.worker import SignalProcessingWorker
from shared import protocol


class ReducerV2(SignalProcessingWorker):
    """
    Flexible Reducer for Q2, Q3 and Q4.
    It waits for a NOTI signal (from the Grouper),
    merges all partial CSV files found in `input_dir`,
    writes a single reduced file to `output_file`,
    and finally sends a NOTI signal to the next worker (Topper).
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, input_dir, output_file, reducer_mode):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.input_dir = input_dir
        self.output_file = output_file
        self.reducer_mode = reducer_mode.lower()

    # ======================================================
    # CORE: Executed when a NOTI signal is received
    # ======================================================

    def _process_signal(self):
        """
        Called automatically when a NOTI signal arrives.
        Reads partial CSVs, applies the proper reduction strategy,
        and writes the final merged output.
        """
        if not os.path.exists(self.input_dir):
            logging.warning(f"[Reducer {self.reducer_mode.upper()}] Input directory not found: {self.input_dir}")
            return

        # Select reduction logic based on mode
        if self.reducer_mode == "q2":
            combined = self._reduce_q2()
        elif self.reducer_mode == "q3":
            combined = self._reduce_q3()
        elif self.reducer_mode == "q4":
            combined = self._reduce_q4()
        else:
            logging.error(f"[Reducer] Unknown mode: {self.reducer_mode}")
            return

        # Write final merged CSV
        final_file = f"{self.output_file}.csv"
        logging.info(f"[Reducer {self.reducer_mode.upper()}] Writing merged result to {final_file}")
        try:
            with open(final_file, "w", newline="") as f:
                writer = csv.writer(f)
                for row in combined:
                    writer.writerow(row)
            logging.info(f"[Reducer {self.reducer_mode.upper()}] Reduction complete with {len(combined)} records.")
        except Exception as e:
            logging.error(f"[Reducer {self.reducer_mode.upper()}] Error writing final file: {e}")

        gc.collect()

    # ======================================================
    # REDUCTION MODES
    # ======================================================

    def _reduce_q2(self):
        """
        Merge partial GrouperV2 Q2 files:
        item_id,quantity,subtotal → aggregate totals per item.
        """
        combined = defaultdict(lambda: [0, 0.0])
        for filename in os.listdir(self.input_dir):
            if not filename.endswith(".csv"):
                continue
            fpath = os.path.join(self.input_dir, filename)
            logging.info(f"[Reducer Q2] Processing {filename}")
            try:
                with open(fpath, "r") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if len(row) != 3:
                            continue
                        item_id, qty, subtotal = row
                        combined[item_id][0] += int(qty)
                        combined[item_id][1] += float(subtotal)
            except Exception as e:
                logging.error(f"[Reducer Q2] Error reading {filename}: {e}")

        return [(item_id, qty, subtotal) for item_id, (qty, subtotal) in combined.items()]

    def _reduce_q3(self):
        """
        Merge partial GrouperV2 Q3 files:
        store_id,semester,tpv → total TPV per store and semester.
        """
        combined = defaultdict(lambda: defaultdict(float))
        for filename in os.listdir(self.input_dir):
            if not filename.endswith(".csv"):
                continue
            fpath = os.path.join(self.input_dir, filename)
            logging.info(f"[Reducer Q3] Processing {filename}")
            try:
                with open(fpath, "r") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if len(row) != 3:
                            continue
                        store_id, semester, tpv = row
                        combined[store_id][semester] += float(tpv)
            except Exception as e:
                logging.error(f"[Reducer Q3] Error reading {filename}: {e}")

        result = [(store_id, semester, tpv)
                  for store_id, sem_data in combined.items()
                  for semester, tpv in sem_data.items()]
        return result

    def _reduce_q4(self):
        """
        Merge partial GrouperV2 Q4 files:
        user_id,total_purchases → total purchases per user.
        """
        combined = defaultdict(int)
        for filename in os.listdir(self.input_dir):
            if not filename.endswith(".csv"):
                continue
            fpath = os.path.join(self.input_dir, filename)
            logging.info(f"[Reducer Q4] Processing {filename}")
            try:
                with open(fpath, "r") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if len(row) != 2:
                            continue
                        user_id, count = row
                        combined[user_id] += int(count)
            except Exception as e:
                logging.error(f"[Reducer Q4] Error reading {filename}: {e}")

        return [(user_id, count) for user_id, count in combined.items()]


# ======================================================
# MAIN
# ======================================================

if __name__ == "__main__":
    import configparser

    def create_reducer_v2():
        config_path = os.path.join(os.path.dirname(__file__), "config.ini")
        config = configparser.ConfigParser()
        config.read(config_path)

        rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        queue_in = os.environ.get("QUEUE_IN")
        queue_out = os.environ.get("COMPLETION_QUEUE")
        reducer_mode = os.environ.get("REDUCER_MODE", "q2")
        input_dir = os.environ.get("INPUT_DIR", "/app/temp/grouper_q2")
        output_file = os.environ.get("OUTPUT_FILE", "/app/temp/reduced_output")

        return ReducerV2(queue_in, queue_out, rabbitmq_host, input_dir, output_file, reducer_mode)

    ReducerV2.run_worker_main(create_reducer_v2)

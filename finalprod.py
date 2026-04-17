import os
import re
import json
import shutil
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import platform
from collections import defaultdict


PERL_PATH = os.path.join("exiftool_files", "perl.exe")
EXIFTOOL_SCRIPT = os.path.join("exiftool_files", "exiftool.pl")

MEDIA_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tiff",
    ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".webm", ".m4v", ".3gp"
)


class TakeoutFixerApp:

    def __init__(self, root):

        self.root = root
        self.root.title("Ultimate Google Takeout Fixer")
        self.root.geometry("1100x850")
        self.root.configure(bg="#121212")

        self.input_folder = ""
        self.output_folder = ""

        self.cpu_name = platform.processor()
        self.cpu_threads = multiprocessing.cpu_count()

        self.audit_records = []

        self.setup_ui()

    def make_card(self, parent):
        return tk.Frame(parent, bg="#1E1E1E", bd=1, relief="solid")

    def log(self, text):
        self.log_box.insert(tk.END, text + "\n")
        self.log_box.see(tk.END)
        self.root.update()

    def setup_ui(self):

        tk.Label(
            self.root,
            text="Ultimate Google Takeout Fixer",
            font=("Segoe UI", 22, "bold"),
            bg="#121212",
            fg="white"
        ).pack(pady=15)

        hardware_card = self.make_card(self.root)
        hardware_card.pack(fill="x", padx=10, pady=5)

        tk.Label(
            hardware_card,
            text=f"CPU: {self.cpu_name}",
            bg="#1E1E1E",
            fg="white"
        ).pack(anchor="w", padx=10)

        tk.Label(
            hardware_card,
            text=f"Threads: {self.cpu_threads}",
            bg="#1E1E1E",
            fg="white"
        ).pack(anchor="w", padx=10)

        folder_card = self.make_card(self.root)
        folder_card.pack(fill="x", padx=10, pady=5)

        tk.Button(
            folder_card,
            text="Select Input Folder",
            command=self.select_input,
            bg="#333",
            fg="white"
        ).pack(pady=5)

        self.input_label = tk.Label(
            folder_card,
            text="No Input Selected",
            bg="#1E1E1E",
            fg="white"
        )
        self.input_label.pack()

        tk.Button(
            folder_card,
            text="Select Output Folder",
            command=self.select_output,
            bg="#333",
            fg="white"
        ).pack(pady=5)

        self.output_label = tk.Label(
            folder_card,
            text="No Output Selected",
            bg="#1E1E1E",
            fg="white"
        )
        self.output_label.pack()

        settings_card = self.make_card(self.root)
        settings_card.pack(fill="x", padx=10, pady=5)

        tk.Label(
            settings_card,
            text="Processing Mode",
            bg="#1E1E1E",
            fg="white"
        ).pack()

        self.mode_var = tk.StringVar()

        self.mode_dropdown = ttk.Combobox(
            settings_card,
            textvariable=self.mode_var,
            state="readonly"
        )
        self.mode_dropdown.pack()

        self.mode_dropdown["values"] = [
            "Safe (2 Threads)",
            "Balanced (4 Threads)",
            f"Fast ({min(8,self.cpu_threads)} Threads)",
            f"Maximum ({max(1,self.cpu_threads-1)} Threads)",
            "Custom"
        ]

        self.mode_dropdown.current(1)

        tk.Label(
            settings_card,
            text="Custom Thread Count",
            bg="#1E1E1E",
            fg="white"
        ).pack()

        self.custom_threads_entry = tk.Entry(settings_card)
        self.custom_threads_entry.insert(0, "4")
        self.custom_threads_entry.pack()

        tk.Button(
            settings_card,
            text="START",
            bg="green",
            fg="white",
            font=("Arial", 12, "bold"),
            command=self.start_processing
        ).pack(pady=10)

        self.progress = ttk.Progressbar(self.root, length=900)
        self.progress.pack(pady=10)

        log_card = self.make_card(self.root)
        log_card.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_box = tk.Text(
            log_card,
            bg="#0D0D0D",
            fg="#00FFAA"
        )
        self.log_box.pack(side="left", fill="both", expand=True)

        scrollbar = tk.Scrollbar(log_card)
        scrollbar.pack(side="right", fill="y")

        self.log_box.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.log_box.yview)

    def select_input(self):
        self.input_folder = filedialog.askdirectory()
        self.input_label.config(text=self.input_folder)

    def select_output(self):
        self.output_folder = filedialog.askdirectory()
        self.output_label.config(text=self.output_folder)

    def get_worker_count(self):

        mode = self.mode_var.get()

        if mode == "Custom":

            try:
                return int(self.custom_threads_entry.get())
            except:
                return 4

        return int(mode.split("(")[1].split()[0])

    def shared_prefix_len(self, a, b):

        count = 0

        for x, y in zip(a, b):

            if x == y:
                count += 1
            else:
                break

        return count

    def build_json_index(self, json_files):

        self.exact_json_map = {}
        self.json_base_list = []

        for json_file in json_files:

            self.exact_json_map[json_file] = json_file

            json_base = os.path.splitext(json_file)[0]

            self.json_base_list.append((json_file, json_base))

    def find_matching_json(self, media_file):

        exact_name = media_file + ".supplemental-metadata.json"

        if exact_name in self.exact_json_map:
            return exact_name

        for json_file in self.exact_json_map:

            if json_file.startswith(media_file):
                return json_file

        best_match = None
        best_score = 0

        media_base = os.path.splitext(media_file)[0]

        for json_file, json_base in self.json_base_list:

            score = self.shared_prefix_len(media_base, json_base)

            if score > best_score:

                best_score = score
                best_match = json_file

        if best_score >= len(media_base) * 0.65:
            return best_match

        return None

    def extract_best_date(self, filename, json_data=None):

        for pattern in [
            r'(\d{8})_(\d{6})',
            r'(\d{8})-(\d{6})'
        ]:

            match = re.search(pattern, filename)

            if match:

                try:
                    return datetime.strptime(
                        match.group(1)+match.group(2),
                        "%Y%m%d%H%M%S"
                    )
                except:
                    pass

        trusted_prefixes = [
            "FB_IMG_",
            "IMG_",
            "VID_",
            "SCREENSHOT_",
            "PXL_"
        ]

        for prefix in trusted_prefixes:

            if filename.upper().startswith(prefix):

                milli_match = re.search(r'(\d{13})', filename)

                if milli_match:

                    try:

                        dt = datetime.fromtimestamp(
                            int(milli_match.group(1))/1000
                        )

                        if 2005 <= dt.year <= 2035:
                            return dt

                    except:
                        pass

        if json_data:

            try:
                if "photoTakenTime" in json_data:
                    return datetime.utcfromtimestamp(
                        int(json_data["photoTakenTime"]["timestamp"])
                    )
            except:
                pass

            try:
                if "creationTime" in json_data:
                    return datetime.utcfromtimestamp(
                        int(json_data["creationTime"]["timestamp"])
                    )
            except:
                pass

        return None

    def apply_date(self, filepath, dt):

        formatted = dt.strftime("%Y:%m:%d %H:%M:%S")

        subprocess.run([
            PERL_PATH,
            EXIFTOOL_SCRIPT,
            f"-DateTimeOriginal={formatted}",
            f"-CreateDate={formatted}",
            f"-ModifyDate={formatted}",
            "-overwrite_original",
            filepath
        ], capture_output=True)

        os.utime(filepath, (dt.timestamp(), dt.timestamp()))

    def process_single_file(self, media_file):

        failed_folder = os.path.join(self.output_folder, "failed")

        try:

            media_path = os.path.join(self.input_folder, media_file)
            output_path = os.path.join(self.output_folder, media_file)

            shutil.copy2(media_path, output_path)

            matching_json = self.find_matching_json(media_file)

            json_data = None

            if matching_json:

                with open(
                    os.path.join(self.input_folder, matching_json),
                    "r",
                    encoding="utf-8"
                ) as f:

                    json_data = json.load(f)

            dt = self.extract_best_date(media_file, json_data)

            if dt:

                self.apply_date(output_path, dt)

                self.audit_records.append((dt, media_file))

                return f"SUCCESS: {media_file}"

            else:

                shutil.move(
                    output_path,
                    os.path.join(failed_folder, media_file)
                )

                return f"FAILED: {media_file}"

        except Exception as e:

            return f"FAILED: {media_file} -> {e}"

    def write_audit_report(self):

        audit_path = os.path.join(self.output_folder, "date_audit.txt")

        self.audit_records.sort()

        grouped = defaultdict(int)

        for dt, _ in self.audit_records:
            grouped[dt.strftime("%Y-%m-%d")] += 1

        with open(audit_path, "w", encoding="utf-8") as f:

            f.write("===== DATE SUMMARY =====\n\n")

            for date, count in grouped.items():
                f.write(f"{date} : {count} files\n")

            f.write("\n\n===== FULL LIST =====\n\n")

            for dt, name in self.audit_records:
                f.write(f"{dt.strftime('%Y-%m-%d %H:%M:%S')} -> {name}\n")

    def start_processing(self):

        self.audit_records.clear()

        failed_folder = os.path.join(self.output_folder, "failed")
        os.makedirs(failed_folder, exist_ok=True)

        files = os.listdir(self.input_folder)

        media_files = [
            f for f in files
            if f.lower().endswith(MEDIA_EXTENSIONS)
        ]

        json_files = [
            f for f in files
            if f.lower().endswith(".json")
        ]

        self.build_json_index(json_files)

        workers = self.get_worker_count()

        self.progress["maximum"] = len(media_files)

        with ThreadPoolExecutor(max_workers=workers) as executor:

            futures = [
                executor.submit(self.process_single_file, media)
                for media in media_files
            ]

            for i, future in enumerate(as_completed(futures)):

                result = future.result()

                self.log(result)

                self.progress["value"] = i + 1

                self.root.update()

        self.write_audit_report()

        messagebox.showinfo("Done", "Finished Processing!")


root = tk.Tk()
app = TakeoutFixerApp(root)
root.mainloop()
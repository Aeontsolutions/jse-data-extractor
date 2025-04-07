import tkinter as tk
from tkinter import ttk, messagebox, Menu # Removed scrolledtext
import sqlite3
import os
import boto3
from dotenv import load_dotenv
import csv # Added for CSV parsing
import io  # Added for StringIO
import logging

# --- Configuration (Unchanged) ---
DB_NAME = "jse_financial_data.db"
load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = "jse-renamed-docs"

# --- Database Functions (Unchanged) ---
def get_symbols(db_path):
    symbols = []
    if not os.path.exists(db_path): return symbols
    try:
        conn = sqlite3.connect(db_path); cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'jse_raw_%';")
        symbols = sorted([name[0].replace('jse_raw_', '') for name in cursor.fetchall()])
    except sqlite3.Error as e: messagebox.showerror("DB Error", f"Query symbols failed: {e}")
    finally: conn and conn.close()
    return symbols

def get_years_for_symbol(db_path, symbol):
    years = []
    if not symbol or not os.path.exists(db_path): return years
    table_name = f"jse_raw_{symbol}"
    try:
        conn = sqlite3.connect(db_path); cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT year FROM {table_name} ORDER BY year DESC;")
        years = [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e: messagebox.showerror("DB Error", f"Fetch years failed for {symbol}: {e}")
    finally: conn and conn.close()
    return years

def get_statements_for_year(db_path, symbol, year):
    statements = []
    if not symbol or not year or not os.path.exists(db_path): return statements
    table_name = f"jse_raw_{symbol}"
    try:
        conn = sqlite3.connect(db_path); cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT statement, report_date, period, period_type, group_or_company_level, csv_path, trailing_zeros FROM {table_name} WHERE year = ? ORDER BY report_date DESC, statement;", (year,))
        statements = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
    except sqlite3.Error as e: messagebox.showerror("DB Error", f"Fetch statements failed for {symbol}/{year}: {e}")
    finally: conn and conn.close()
    return statements

def get_line_items(db_path, symbol, csv_path):
    line_items = []
    if not symbol or not csv_path or not os.path.exists(db_path): return line_items
    table_name = f"jse_raw_{symbol}"
    try:
        conn = sqlite3.connect(db_path); cursor = conn.cursor()
        cursor.execute(f"SELECT line_item, line_item_value, period_length FROM {table_name} WHERE csv_path = ? ORDER BY id;", (csv_path,))
        line_items = cursor.fetchall()
    except sqlite3.Error as e: messagebox.showerror("DB Error", f"Fetch line items failed for {csv_path}: {e}")
    finally: conn and conn.close()
    return line_items

# --- S3 Function (Unchanged) ---
def fetch_csv_content(s3_client, bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content_bytes = response['Body'].read()
        try: return content_bytes.decode('utf-8')
        except UnicodeDecodeError: return content_bytes.decode('latin-1')
    except Exception as e:
        messagebox.showerror("S3 Error", f"Failed to fetch S3 object s3://{bucket}/{key}\nError: {e}")
        return None

# --- GUI Application ---
class JseDataViewerApp(tk.Tk):
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        if not os.path.exists(self.db_path):
             messagebox.showerror("Error", f"Database file '{self.db_path}' not found.")
             self.destroy(); return

        self.title("JSE Financial Data Viewer")
        self.geometry("1300x750") # Made slightly larger

        # Internal State (Unchanged)
        self.current_symbol = tk.StringVar(value="Select Symbol")
        self.current_year = tk.StringVar(value="Select Year")
        self.current_statement_info = None
        self.available_statements = []

        # S3 Client (Unchanged)
        self.s3_client = None
        try:
            if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
                 self.s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
            else: self.s3_client = boto3.client('s3', region_name=AWS_REGION)
        except Exception as e: messagebox.showwarning("S3 Warning", f"S3 client init failed. CSV view disabled.\nError: {e}"); self.s3_client = None

        # Menu Bar (Unchanged)
        self.menu_bar = Menu(self); self.config(menu=self.menu_bar)
        self.symbol_menu = Menu(self.menu_bar, tearoff=0); self.menu_bar.add_cascade(label="Symbol", menu=self.symbol_menu, state="disabled")
        self.year_menu = Menu(self.menu_bar, tearoff=0); self.menu_bar.add_cascade(label="Year", menu=self.year_menu, state="disabled")
        self.statement_menu = Menu(self.menu_bar, tearoff=0); self.menu_bar.add_cascade(label="Statement", menu=self.statement_menu, state="disabled")

        # Main Layout (Unchanged)
        self.paned_window = ttk.PanedWindow(self, orient=tk.HORIZONTAL); self.paned_window.pack(expand=True, fill=tk.BOTH, padx=10, pady=10)

        # Left Frame (Metadata + Line Items - Unchanged)
        self.left_frame = ttk.Frame(self.paned_window, padding=5); self.paned_window.add(self.left_frame, weight=1)
        self.metadata_label = ttk.Label(self.left_frame, text="Select Statement from Menu", justify=tk.LEFT, anchor="nw", relief="groove", padding=5); self.metadata_label.pack(fill=tk.X, pady=(0, 10))
        self.tree = ttk.Treeview(self.left_frame, show='headings', columns=("Line Item", "Value", "Period Len"))
        self.tree.heading("Line Item", text="Line Item"); self.tree.heading("Value", text="Value"); self.tree.heading("Period Len", text="Period Len")
        self.tree.column("Line Item", width=300, anchor=tk.W); self.tree.column("Value", width=120, anchor=tk.E); self.tree.column("Period Len", width=80, anchor=tk.CENTER)
        tree_vsb = ttk.Scrollbar(self.left_frame, orient="vertical", command=self.tree.yview); tree_hsb = ttk.Scrollbar(self.left_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=tree_vsb.set, xscrollcommand=tree_hsb.set); tree_vsb.pack(side=tk.RIGHT, fill=tk.Y); tree_hsb.pack(side=tk.BOTTOM, fill=tk.X); self.tree.pack(expand=True, fill=tk.BOTH)

        # *** Right Frame (CSV View as Treeview) ***
        self.right_frame = ttk.Frame(self.paned_window, padding=5)
        self.paned_window.add(self.right_frame, weight=2) # Give CSV view more initial weight

        ttk.Label(self.right_frame, text="Original CSV Content (Table View):").pack(anchor="w")
        # Create Treeview for CSV data
        self.csv_tree = ttk.Treeview(self.right_frame, show='headings') # Start with no columns defined

        # Scrollbars for CSV Treeview
        csv_vsb = ttk.Scrollbar(self.right_frame, orient="vertical", command=self.csv_tree.yview)
        csv_hsb = ttk.Scrollbar(self.right_frame, orient="horizontal", command=self.csv_tree.xview)
        self.csv_tree.configure(yscrollcommand=csv_vsb.set, xscrollcommand=csv_hsb.set)

        csv_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        csv_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.csv_tree.pack(expand=True, fill=tk.BOTH)

        # Initial Population (Unchanged)
        self.populate_symbol_menu()

    # --- Menu Population/Selection Functions (Unchanged) ---
    def populate_symbol_menu(self):
        symbols = get_symbols(self.db_path)
        if not symbols: messagebox.showerror("Error", "No data tables found."); self.menu_bar.entryconfig("Symbol", state="disabled"); return
        self.symbol_menu.delete(0, tk.END)
        for symbol in symbols: self.symbol_menu.add_command(label=symbol, command=lambda s=symbol: self.select_symbol(s))
        self.menu_bar.entryconfig("Symbol", state="normal")

    def select_symbol(self, symbol):
        self.current_symbol.set(symbol); self.title(f"JSE Viewer - {symbol}")
        self.current_year.set("Select Year"); self.current_statement_info = None; self.available_statements = []
        self.year_menu.delete(0, tk.END); self.statement_menu.delete(0, tk.END)
        self.menu_bar.entryconfig("Year", state="disabled"); self.menu_bar.entryconfig("Statement", state="disabled")
        self.clear_displays()
        years = get_years_for_symbol(self.db_path, symbol)
        if years:
            for year in years: self.year_menu.add_command(label=str(year), command=lambda y=year: self.select_year(y))
            self.menu_bar.entryconfig("Year", state="normal")
        else: messagebox.showinfo("No Data", f"No years found for {symbol}.")

    def select_year(self, year):
        self.current_year.set(str(year))
        self.current_statement_info = None; self.statement_menu.delete(0, tk.END)
        self.menu_bar.entryconfig("Statement", state="disabled"); self.clear_displays()
        symbol = self.current_symbol.get()
        self.available_statements = get_statements_for_year(self.db_path, symbol, year)
        if self.available_statements:
            for idx, stmt_info in enumerate(self.available_statements):
                 menu_label = f"{stmt_info['statement']} ({stmt_info['report_date']}) ({stmt_info['group_or_company_level']})"
                 self.statement_menu.add_command(label=menu_label, command=lambda index=idx: self.select_statement(index))
            self.menu_bar.entryconfig("Statement", state="normal")
        else: messagebox.showinfo("No Data", f"No statements found for {symbol} in {year}.")

    # --- Statement Selection and Data Loading ---
    def select_statement(self, statement_index):
        """Handles statement selection: loads metadata, line items, and parses/displays CSV."""
        if not self.available_statements or statement_index >= len(self.available_statements): return
        self.current_statement_info = self.available_statements[statement_index]
        self.clear_displays() # Clear previous data first

        # 1. Display Metadata (Unchanged)
        md = self.current_statement_info
        metadata_text = (f"Symbol: {self.current_symbol.get()}\nYear: {self.current_year.get()}\n"
                         f"Statement: {md.get('statement', 'N/A')}\nReport Date: {md.get('report_date', 'N/A')}\n"
                         f"Period: {md.get('period', 'N/A')} ({md.get('period_type', 'N/A')})\nLevel: {md.get('group_or_company_level', 'N/A')}\n"
                         f"Source: {os.path.basename(md.get('csv_path', 'N/A'))}\nTrailing Zeros: {md.get('trailing_zeros', 'N/A')}\n")
        self.metadata_label.config(text=metadata_text)

        # 2. Display Line Items (Unchanged)
        csv_path = md.get('csv_path')
        if csv_path:
             symbol = self.current_symbol.get(); line_items = get_line_items(self.db_path, symbol, csv_path)
             if line_items:
                 for li, val, pl in line_items:
                     formatted_value = f"{val:,.2f}" if isinstance(val, (int, float)) else val
                     self.tree.insert("", tk.END, values=(li, formatted_value, pl))

        # 3. Fetch, Parse, and Display CSV Content in Treeview
        if self.s3_client and csv_path:
            csv_content = fetch_csv_content(self.s3_client, S3_BUCKET, csv_path)
            self.update_csv_treeview(csv_content, csv_path) # Call dedicated function
        elif not self.s3_client:
             self.update_csv_treeview("<S3 client not available>", csv_path)
        else:
             self.update_csv_treeview(None, csv_path) # Handle case where path missing


    def update_csv_treeview(self, csv_content, csv_path):
        """Clears, configures, and populates the CSV Treeview."""
        # Destroy and recreate the CSV Treeview to avoid column configuration issues
        self.csv_tree.destroy()
        
        # Create new Treeview
        self.csv_tree = ttk.Treeview(self.right_frame, show='headings')
        csv_vsb = ttk.Scrollbar(self.right_frame, orient="vertical", command=self.csv_tree.yview)
        csv_hsb = ttk.Scrollbar(self.right_frame, orient="horizontal", command=self.csv_tree.xview)
        self.csv_tree.configure(yscrollcommand=csv_vsb.set, xscrollcommand=csv_hsb.set)
        
        # Pack the new widgets
        csv_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        csv_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.csv_tree.pack(expand=True, fill=tk.BOTH)

        # --- Handle No Content ---
        if csv_content is None:
            if csv_path:
                # Define a single "Status" column for the message
                self.csv_tree['columns'] = ('Status',)
                self.csv_tree.heading('Status', text='Status')
                self.csv_tree.column('Status', width=300)
                self.csv_tree.insert("", tk.END, values=(f"<Could not load CSV: {os.path.basename(csv_path)}>",))
            # else: leave tree empty if no path provided
            return
        if isinstance(csv_content, str) and csv_content.startswith("<"):
            self.csv_tree['columns'] = ('Status',)
            self.csv_tree.heading('Status', text='Status')
            self.csv_tree.column('Status', width=300)
            self.csv_tree.insert("", tk.END, values=(csv_content,))
            return

        # --- Parse and Populate ---
        try:
            csvfile = io.StringIO(csv_content)
            # Try to sniff dialect, especially for quote handling, but default is usually fine
            # dialect = csv.Sniffer().sniff(csvfile.read(1024)); csvfile.seek(0)
            # reader = csv.reader(csvfile, dialect)
            reader = csv.reader(csvfile)

            header = next(reader, None)
            if not header:
                self.csv_tree['columns'] = ('Status',)
                self.csv_tree.heading('Status', text='Status')
                self.csv_tree.column('Status', width=300)
                self.csv_tree.insert("", tk.END, values=("<CSV appears empty or has no header>",))
                return

            # --- Directly set the new columns and headings ---
            self.csv_tree['columns'] = tuple(header)
            self.csv_tree['displaycolumns'] = tuple(header) # Or '#all'

            for col in header:
                self.csv_tree.heading(col, text=col)
                self.csv_tree.column(col, width=100, anchor=tk.W, stretch=True)

            # Insert data rows
            for row_num, row in enumerate(reader):
                if len(row) == len(header):
                    self.csv_tree.insert("", tk.END, values=row)
                else:
                    logging.warning(f"Skipping malformed row {row_num+2} in {csv_path}: Expected {len(header)} cols, got {len(row)}")
                    # Handle padding if necessary, but skipping might be safer
                    # padded_row = row[:len(header)] + [''] * (len(header) - len(row))
                    # self.csv_tree.insert("", tk.END, values=padded_row)


        except csv.Error as e:
            logging.error(f"CSV Parsing Error for {csv_path}: {e}", exc_info=True)
            # Define error column and display message
            self.csv_tree['columns'] = ('Error',)
            self.csv_tree.heading('Error', text='CSV Parsing Error')
            self.csv_tree.column('Error', width=400)
            self.csv_tree.insert("", tk.END, values=(f"Error reading CSV: {e}",))
        except Exception as e:
            logging.error(f"Unexpected Error processing CSV Treeview for {csv_path}: {e}", exc_info=True)
            # Define error column and display message
            self.csv_tree['columns'] = ('Error',)
            self.csv_tree.heading('Error', text='Processing Error')
            self.csv_tree.column('Error', width=400)
            self.csv_tree.insert("", tk.END, values=(f"Unexpected error: {e}",))


    def clear_displays(self):
        """Clears the data display areas."""
        self.metadata_label.config(text="Select Statement from Menu")
        # Clear line item tree rows
        for item in self.tree.get_children():
            self.tree.delete(item)
        # Clear CSV tree rows ONLY. Columns will be redefined when new data loads.
        for item in self.csv_tree.get_children():
            self.csv_tree.delete(item)
        # DO NOT reset columns here, as it causes the TclError.
        # self.csv_tree["columns"] = () # <-- REMOVE THIS LINE
        # self.csv_tree["displaycolumns"] = () # <-- REMOVE THIS LINE (optional, but cleaner to remove)


if __name__ == "__main__":
    # Configure basic logging if needed for GUI errors (optional)
    # logging.basicConfig(level=logging.WARNING, format='%(asctime)s:%(levelname)s:%(message)s')
    app = JseDataViewerApp(DB_NAME)
    app.mainloop()
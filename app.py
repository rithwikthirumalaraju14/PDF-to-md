from flask import Flask, request, render_template, send_file
from arango import ArangoClient
import os
import json
import re
from werkzeug.utils import secure_filename
import mistune

# ========================
# Flask Setup
# ========================
app = Flask(__name__)
UPLOAD_FOLDER = 'Uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ========================
# ArangoDB Setup
# ========================
ARANGO_HOST = 'http://localhost:8529'  # Update if remote
ARANGO_USERNAME = 'root'  # Replace with your username
ARANGO_PASSWORD = ''  # Replace with your password
ARANGO_DB_NAME = '_system'

def init_arango():
    """
    Initialize ArangoDB client and database.
    Returns the database object.
    """
    try:
        client = ArangoClient(hosts=ARANGO_HOST)
        sys_db = client.db('_system', username=ARANGO_USERNAME, password=ARANGO_PASSWORD)
        
        # Create database if it doesn't exist
        if not sys_db.has_database(ARANGO_DB_NAME):
            sys_db.create_database(ARANGO_DB_NAME)
        
        return client.db(ARANGO_DB_NAME, username=ARANGO_USERNAME, password=ARANGO_PASSWORD)
    
    except Exception as e:
        raise Exception(f"Failed to initialize ArangoDB: {str(e)}")

# ========================
# Table Extraction
# ========================
def extract_markdown_tables(md_content):
    """
    Extract tables from Markdown and convert to structured data.
    Returns a list of tables, each table as a list of dicts (rows).
    """
    tables = []
    current_table = []
    headers = []
    in_table = False

    lines = md_content.splitlines()
    for line in lines:
        if '|' in line and not re.match(r'^\s*\|[-:|\s]+\|\s*$', line.strip()):
            cells = [cell.strip() for cell in line.split('|') if cell.strip()]
            if cells:
                if not in_table and not headers:
                    headers = cells
                    in_table = True
                elif in_table:
                    if len(cells) >= len(headers):
                        cells = cells[:len(headers)]
                    else:
                        cells.extend([''] * (len(headers) - len(cells)))
                    row = dict(zip(headers, cells))
                    current_table.append(row)
        elif in_table and re.match(r'^\s*\|[-:|\s]+\|\s*$', line.strip()):
            continue
        elif in_table and (line.strip() == '' or '|' not in line):
            if current_table:
                tables.append(current_table)
                current_table = []
                headers = []
                in_table = False

    if current_table:
        tables.append(current_table)
    
    return tables

# ========================
# Routes
# ========================
@app.route('/', methods=['GET', 'POST'])
def upload_md():
    if request.method == 'POST':
        if 'markdown' not in request.files:
            return render_template('upload.html', error="No file uploaded")

        file = request.files['markdown']
        if not file or not file.filename.endswith('.md'):
            return render_template('upload.html', error="Please upload a valid .md file")

        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        try:
            # Read and validate Markdown content
            with open(filepath, 'r', encoding='utf-8') as f:
                md_content = f.read()

            markdown = mistune.create_markdown()
            markdown(md_content)  # Validate Markdown

            # Extract tables
            tables = extract_markdown_tables(md_content)
            if not tables:
                return render_template('upload.html', error="No tables found in the Markdown file")

            # Save tables as JSON
            tables_path = os.path.join(app.config['UPLOAD_FOLDER'], 'tables_only.json')
            with open(tables_path, 'w', encoding='utf-8') as json_file:
                json.dump(tables, json_file, indent=4, ensure_ascii=False)

            # Insert tables into ArangoDB
            try:
                db = init_arango()
                for i, table in enumerate(tables):
                    # Create unique collection name (e.g., table_1)
                    collection_name = f'table_{i + 1}'
                    
                    # Create collection if it doesn't exist
                    if not db.has_collection(collection_name):
                        db.create_collection(collection_name)
                    
                    collection = db.collection(collection_name)
                    
                    # Insert each row as a document
                    for row_idx, row in enumerate(table):
                        # Add metadata
                        row_copy = row.copy()
                        row_copy['_source_file'] = filename
                        row_copy['_row_index'] = row_idx
                        # Use a unique key to avoid conflicts
                        row_copy['_key'] = f'{filename.replace(".md", "")}_{i + 1}_{row_idx}'
                        collection.insert(row_copy, overwrite=True)

            except Exception as e:
                return render_template('upload.html', error=f"Failed to insert into ArangoDB: {str(e)}")

            return render_template('result.html', tables=tables)

        except Exception as e:
            return render_template('upload.html', error=f"Error processing file: {str(e)}")

    return render_template('upload.html')

@app.route('/download')
def download_json():
    path = os.path.join(app.config['UPLOAD_FOLDER'], 'tables_only.json')
    if not os.path.exists(path):
        return render_template('upload.html', error="No JSON file available for download")
    return send_file(path, as_attachment=True, download_name='tables_only.json')

# ========================
# Run App
# ========================
if __name__ == '__main__':
    app.run(debug=True)

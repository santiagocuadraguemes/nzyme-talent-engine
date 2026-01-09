import os

def redact_env_content(content):
    """
    Recibe el contenido de texto de un .env y sustituye los valores por ******
    """
    lines = content.splitlines()
    redacted_lines = []
    
    for line in lines:
        stripped = line.strip()
        # Si es comentario o linea vacia, se deja igual
        if not stripped or stripped.startswith("#"):
            redacted_lines.append(line)
            continue
            
        # Si tiene un =, asumimos que es CLAVE=VALOR
        if "=" in line:
            key, _ = line.split("=", 1)
            # Mantenemos la clave, ocultamos el valor
            redacted_lines.append(f"{key}=******")
        else:
            redacted_lines.append(line)
            
    return "\n".join(redacted_lines)

def export_project_code():
    output_filename = "project_code_dump.txt"
    
    # Carpetas que ignoramos
    ignore_dirs = {
        "venv", 
        ".venv", 
        "__pycache__", 
        ".git", 
        ".vscode", 
        ".idea",
        "cvs_entrada",
        "cvs_salida"
    }
    
    # Archivos que queremos leer
    include_extensions = {".py", ".txt", ".env", ".md", ".json"}

    print(f"Generando resumen SEGURO (sin claves) en {output_filename}...")

    with open(output_filename, "w", encoding="utf-8") as outfile:
        outfile.write("--- SNAPSHOT DEL PROYECTO (CENSURADO) ---\n\n")

        for root, dirs, files in os.walk("."):
            # Filtrar directorios ignorados
            dirs[:] = [d for d in dirs if d not in ignore_dirs]

            for file in files:
                if any(file.endswith(ext) for ext in include_extensions):
                    # No incluir este mismo script ni el output
                    if file == os.path.basename(__file__) or file == output_filename:
                        continue

                    filepath = os.path.join(root, file)
                    
                    outfile.write("="*60 + "\n")
                    outfile.write(f"RUTA: {filepath}\n")
                    outfile.write("="*60 + "\n")

                    try:
                        with open(filepath, "r", encoding="utf-8") as infile:
                            content = infile.read()
                            
                            # SI ES EL .ENV, LO CENSURAMOS
                            if file == ".env":
                                content = redact_env_content(content)
                                outfile.write("# [CONTENIDO CENSURADO POR SEGURIDAD]\n")
                            
                            outfile.write(content)
                    except Exception as e:
                        outfile.write(f"[ERROR LEYENDO ARCHIVO: {e}]")
                    
                    outfile.write("\n\n")

    print(f"Hecho. Se ha creado '{output_filename}' con las claves ocultas.")

if __name__ == "__main__":
    export_project_code()
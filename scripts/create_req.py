import subprocess

# Lista de librerías específicas
libraries = ["pandas", "matplotlib", "mysql-connector-python", "snowflake-connector-python", "mage-ai"]

# Obtener versiones de las librerías
with open("requirements.txt", "w") as f:
    for lib in libraries:
        result = subprocess.run(["pip", "show", lib], capture_output=True, text=True)
        for line in result.stdout.split("\n"):
            if line.startswith("Version:"):
                version = line.split(" ")[1]
                f.write(f"{lib}=={version}\n")

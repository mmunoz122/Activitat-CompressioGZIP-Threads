import os
import gzip
import shutil
import time
from threading import Thread, Lock


# Funció per mesurar el temps d'execució
def mesurar_temps(func, *args):
    temps_inici = time.time()
    func(*args)
    temps_final = time.time()
    return temps_final - temps_inici


# Comprovar si el directori existeix i és accessible
def comprovar_directori(path, permisos='r'):
    if not os.path.exists(path):
        raise FileNotFoundError(f"El directori {path} no existeix.")
    if permisos == 'r' and not os.access(path, os.R_OK):
        raise PermissionError(f"No tenim permisos de lectura en el directori {path}.")
    if permisos == 'w' and not os.access(path, os.W_OK):
        raise PermissionError(f"No tenim permisos d'escriptura en el directori {path}.")
    if not os.path.isdir(path):
        raise NotADirectoryError(f"{path} no és un directori.")
    print(f"Directori {path} verificat correctament.")


# Crear un directori de destí si no existeix
def crear_directori(dest_path):
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
        print(f"Creat el directori: {dest_path}")
    elif not os.access(dest_path, os.W_OK):
        raise PermissionError(f"Accés denegat al directori de destí: {dest_path}")


# Funció per comprimir un fitxer en format GZIP
def comprimir_arxiu(file_path, dest_path):
    try:
        with gzip.open(dest_path, 'wb') as f_out:
            with open(file_path, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)
        print(f"Arxiu comprimit: {file_path}")
    except Exception as e:
        print(f"Error en comprimir {file_path}: {e}")


# Funció per guardar el nom dels subdirectoris en un arxiu
def guardar_subdirectoris(directori_origen, directori_destin):
    subdirs = [d for d in os.listdir(directori_origen) if os.path.isdir(os.path.join(directori_origen, d))]
    with open(os.path.join(directori_destin, "directoris.txt"), "w") as file:
        for subdir in subdirs:
            file.write(subdir + "\n")
    print("Noms dels subdirectoris guardats en directoris.txt.")


# Funció per comprimir el directori amb múltiples "threads"
def comprimir_directori(directori_origen, directori_destin, max_threads):
    threads = []
    lock = Lock()

    # Llista de tots els arxius en el directori d'origen (sense subdirectoris)
    files = [f for f in os.listdir(directori_origen) if os.path.isfile(os.path.join(directori_origen, f))]

    # Funció interna per comprimir un fitxer amb un "thread"
    def comprimir_thread(file):
        with lock:
            file_path = os.path.join(directori_origen, file)
            dest_path = os.path.join(directori_destin, f"{file}.gz")
            comprimir_arxiu(file_path, dest_path)

    # Crearem "threads" per comprimir els arxius
    for file in files:
        while len(threads) >= max_threads:
            # Esperarem que algun "thread" acabi abans de crear-ne un altre
            threads = [t for t in threads if t.is_alive()]

        thread = Thread(target=comprimir_thread, args=(file,))
        thread.start()
        threads.append(thread)

    # Esperarem que tots els "threads" acabin
    for thread in threads:
        thread.join()


# Sol·licitarem dades a l'usuari
def solicitar_dades():
    directori_origen = input("Introdueix el path del directori d'origen: ")
    comprovar_directori(directori_origen, 'r')

    directori_destin = input("Introdueix el path del directori de destí: ")
    comprovar_directori(directori_destin, 'w')
    crear_directori(directori_destin)

    max_threads = int(input("Introdueix el nombre màxim de 'threads': "))
    return directori_origen, directori_destin, max_threads


# Funció principal
def main():
    try:
        # Demanem les dades necessàries a l'usuari
        directori_origen, directori_destin, max_threads = solicitar_dades()

        # Guardem els subdirectoris en un fitxer
        guardar_subdirectoris(directori_origen, directori_destin)

        # Mesurem el temps d'execució de la compressió
        temps = mesurar_temps(comprimir_directori, directori_origen, directori_destin, max_threads)
        print(f"Temps d'execució amb {max_threads} 'threads': {temps} segons.")

    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()

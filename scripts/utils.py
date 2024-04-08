import hashlib

def create_unique_id(fecha, codigo):
    entrada_unica = f"{fecha}-{codigo}"
    hash_obj = hashlib.sha256(entrada_unica.encode())
    id_unico = hash_obj.hexdigest()
    return id_unico
from geopy.geocoders import Nominatim
import re

def is_valid_email(email):
    if email is None: return None
    email = email.strip()
    if len(email) == 0: return None
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def remove_whitespace(text):
    if text is None:
        return None
    return "".join(text.split())

def remove_symbols(text):
    return re.sub(r'[^a-zA-Z0-9 ]', '', text)

def remove_non_numeric(text):
    if text is None:
      return None
    return re.sub(r'\D', '', text)

def remove_numeric(text):
    if text is None:
      return None
    return re.sub(r'\d+', '', text)

def is_valid_cpf(cpf):
    if cpf is None:
      return None
    cpf = remove_non_numeric(cpf)
    if len(cpf) != 11 or not cpf.isdigit():
        return "ERROR"
    
    def calculate_digit(digits):
        sum_ = sum(int(digit) * weight for digit, weight in zip(digits, range(len(digits) + 1, 1, -1)))
        re_ = sum_ % 11
        return '0' if re_ < 2 else str(11 - re_)
    
    if cpf[-2:] != calculate_digit(cpf[:9]) + calculate_digit(cpf[:10]):
        return "ERROR"
    
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"

def is_valid_cnpj(cnpj):
    cnpj = remove_non_numeric(cnpj)
    if len(cnpj) != 14 or not cnpj.isdigit():
        return "ERROR"
    
    def calculate_digit(digits, weights):
        sum_ = sum(int(digit) * weight for digit, weight in zip(digits, weights))
        re_ = sum_ % 11
        return '0' if re_ < 2 else str(11 - re_)
    
    first_weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    second_weights = [6] + first_weights

    first_digit = calculate_digit(cnpj[:12], first_weights)
    second_digit = calculate_digit(cnpj[:12] + first_digit, second_weights)
    
    if cnpj[-2:] != first_digit + second_digit:
        return "ERROR"
    
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
   
def trim(string):
    if string is None:
        return None
    return string.strip() if isinstance(string, str) else string

def is_valid_cep(cep):
    if cep is None:
        return None
    cep = remove_non_numeric(cep)
    if len(cep) != 8:
        return "ERROR"
    return f"{cep[:5]}-{cep[5:]}"

def is_valid_phone(phone):
    if phone is None:
        return None
    phone = remove_non_numeric(phone)
    if len(phone) == 10:
        return f"({phone[:2]}) {phone[2:6]}-{phone[6:]}"
    elif len(phone) == 11:
        return f"({phone[:2]}) {phone[2:7]}-{phone[7:]}"
    return "ERROR"

def lat_long(address):
    if address is None:
        return None, None, None
    #time.sleep(1)
    geolocator = Nominatim(user_agent="geoapi", timeout=10)
    loc_ = geolocator.geocode(address)
    
    if loc_:
        lat, lon = loc_.latitude, loc_.longitude
        format_point = f"({lon},{lat})"
        format_geography = f"POINT ({lon} {lat})"
        reverse = geolocator.reverse((lat, lon), exactly_one=True, language="pt")
        return format_point, format_geography, reverse
    else:
        return None, None, None
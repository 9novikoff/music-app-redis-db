import redis
from typing import Optional, Dict, List

r = redis.Redis(host='localhost', port=6379, db=0)

def decode_redis_data(data: Dict[bytes, bytes]) -> Dict[str, str]:
    return {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}

class User:
    @staticmethod
    def create(login: str):
        r.hset(f"user:{login}", mapping={"login": login})

    @staticmethod
    def get(login: str) -> Optional[Dict[str, str]]:
        if r.exists(f"user:{login}"):
            return decode_redis_data(r.hgetall(f"user:{login}"))
        return None

class Author:
    @staticmethod
    def create(name: Optional[str], user: str):
        r.hset(f"author:{user}", mapping={"name": name or "", "user": user})
        r.sadd(f"user:{user}:authors", f"author:{user}")

    @staticmethod
    def get(user: str) -> Optional[Dict[str, str]]:
        if r.exists(f"author:{user}"):
            return decode_redis_data(r.hgetall(f"author:{user}"))
        return None

class AuthorInfo:
    @staticmethod
    def create(biography: Optional[str], awards: Optional[str], photo: Optional[bytes], author: str):
        r.hset(f"author_info:{author}", mapping={
            "biography": biography or "",
            "awards": awards or "",
            "photo": photo or b''
        })

    @staticmethod
    def get(author: str) -> Optional[Dict[str, str]]:
        if r.exists(f"author_info:{author}"):
            return decode_redis_data(r.hgetall(f"author_info:{author}"))
        return None

class Album:
    @staticmethod
    def create(name: str, release_year: str, author: str):
        r.hset(f"album:{name}:{release_year}:{author}", mapping={
            "name": name,
            "release_year": release_year,
            "author": author
        })
        r.sadd(f"author:{author}:albums", f"album:{name}:{release_year}:{author}")

    @staticmethod
    def get(name: str, release_year: str, author: str) -> Optional[Dict[str, str]]:
        key = f"album:{name}:{release_year}:{author}"
        if r.exists(key):
            return decode_redis_data(r.hgetall(key))
        return None

class Track:
    @staticmethod
    def create(name: str, album_name: str, album_year: str, album_author: str, 
               remix_name: Optional[str] = None, remix_album_name: Optional[str] = None, 
               remix_album_year: Optional[str] = None, remix_album_author: Optional[str] = None):
        r.hset(f"track:{name}:{album_name}:{album_year}:{album_author}", mapping={
            "name": name,
            "album_name": album_name,
            "album_year": album_year,
            "album_author": album_author,
            "remix_name": remix_name or "",
            "remix_album_name": remix_album_name or "",
            "remix_album_year": remix_album_year or "",
            "remix_album_author": remix_album_author or ""
        })
        r.sadd(f"album:{album_name}:{album_year}:{album_author}:tracks", 
               f"track:{name}:{album_name}:{album_year}:{album_author}")

    @staticmethod
    def get(name: str, album_name: str, album_year: str, album_author: str) -> Optional[Dict[str, str]]:
        key = f"track:{name}:{album_name}:{album_year}:{album_author}"
        if r.exists(key):
            return decode_redis_data(r.hgetall(key))
        return None

class Rate:
    @staticmethod
    def create(track_name: str, track_album_name: str, track_album_year: str, track_album_author: str, 
               user_login: str, rating: int):
        if not (1 <= rating <= 5):
            raise ValueError("Rating must be between 1 and 5")
        r.hset(f"rate:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:{user_login}", mapping={
            "track_name": track_name,
            "track_album_name": track_album_name,
            "track_album_year": track_album_year,
            "track_album_author": track_album_author,
            "user_login": user_login,
            "rating": rating
        })
        r.sadd(f"track:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:ratings", 
               f"rate:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:{user_login}")
        r.sadd(f"user:{user_login}:ratings", f"rate:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:{user_login}")

    @staticmethod
    def get(track_name: str, track_album_name: str, track_album_year: str, track_album_author: str, 
            user_login: str) -> Optional[Dict[str, str]]:
        key = f"rate:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:{user_login}"
        if r.exists(key):
            return decode_redis_data(r.hgetall(key))
        return None
    
    @staticmethod
    def get_all_ratings_by_user(user_login: str) -> List[Dict[str, str]]:
        rating_keys = r.smembers(f"user:{user_login}:ratings")
        return [decode_redis_data(r.hgetall(key)) for key in rating_keys]

class Device:
    @staticmethod
    def create(mac_address: str):
        r.hset(f"device:{mac_address}", mapping={"mac_address": mac_address})

    @staticmethod
    def get(mac_address: str) -> Optional[Dict[str, str]]:
        if r.exists(f"device:{mac_address}"):
            return decode_redis_data(r.hgetall(f"device:{mac_address}"))
        return None

class Listening:
    @staticmethod
    def create(track_name: str, track_album_name: str, track_album_year: str, track_album_author: str, 
               device_mac_address: str, user_login: str):
        r.hset(f"listening:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:{device_mac_address}:{user_login}", mapping={
            "track_name": track_name,
            "track_album_name": track_album_name,
            "track_album_year": track_album_year,
            "track_album_author": track_album_author,
            "device_mac_address": device_mac_address,
            "user_login": user_login
        })
        r.sadd(f"track:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:listenings", 
               f"listening:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:{device_mac_address}:{user_login}")
        r.sadd(f"user:{user_login}:listenings", f"listening:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:{device_mac_address}:{user_login}")
        r.sadd(f"device:{device_mac_address}:listenings", f"listening:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:{device_mac_address}:{user_login}")

    @staticmethod
    def get(track_name: str, track_album_name: str, track_album_year: str, track_album_author: str, 
            device_mac_address: str, user_login: str) -> Optional[Dict[str, str]]:
        key = f"listening:{track_name}:{track_album_name}:{track_album_year}:{track_album_author}:{device_mac_address}:{user_login}"
        if r.exists(key):
            return decode_redis_data(r.hgetall(key))
        return None

    @staticmethod
    def get_all_listenings_by_user(user_login: str) -> List[Dict[str, str]]:
        listening_keys = r.smembers(f"user:{user_login}:listenings")
        return [decode_redis_data(r.hgetall(key)) for key in listening_keys]

    @staticmethod
    def get_all_listenings_by_device(device_mac_address: str) -> List[Dict[str, str]]:
        listening_keys = r.smembers(f"device:{device_mac_address}:listenings")
        return [decode_redis_data(r.hgetall(key)) for key in listening_keys]

if __name__ == "__main__":
    User.create("john_doe")
    print(User.get("john_doe"))

    Author.create("John Doe", "john_doe")
    print(Author.get("john_doe"))

    AuthorInfo.create("Biography", "Awards", b'photo', "john_doe")
    print(AuthorInfo.get("john_doe"))

    Album.create("Album1", "2022", "john_doe")
    print(Album.get("Album1", "2022", "john_doe"))
    
    Track.create("Track1", "Album1", "2022", "john_doe")
    print(Track.get("Track1", "Album1", "2022", "john_doe"))

    Device.create("00:11:22:33:44:55")
    print(Device.get("00:11:22:33:44:55"))

    Rate.create("Track1", "Album1", "2022", "john_doe", "john_doe", 5)
    print(Rate.get("Track1", "Album1", "2022", "john_doe", "john_doe"))
    print(Rate.get_all_ratings_by_user("john_doe"))

    Listening.create("Track1", "Album1", "2022", "john_doe", "00:11:22:33:44:55", "john_doe")
    print(Listening.get("Track1", "Album1", "2022", "john_doe", "00:11:22:33:44:55", "john_doe"))
    print(Listening.get_all_listenings_by_user("john_doe"))
    print(Listening.get_all_listenings_by_device("00:11:22:33:44:55"))

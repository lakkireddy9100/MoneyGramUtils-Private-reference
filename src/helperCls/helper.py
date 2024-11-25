import ssl

ssl._create_default_https_context = ssl._create_unverified_context

from geopy.geocoders import Nominatim

from src import printcolors as pc
from src import config


class ContentSquare:
    api = None
    api2 = None
    geolocator = Nominatim(user_agent="http")
    user_id = None
    target_id = None
    is_private = True
    target = ""
    writeFile = False
    jsonDump = False
    cli_mode = False
    output_dir = "output"

    def __init__(self, target, is_file, is_json, is_cli, output_dir, clear_cookies):
        self.output_dir = output_dir or self.output_dir
        u = config.getUsername()
        p = config.getPassword()
        self.clear_cookies(clear_cookies)
        self.cli_mode = is_cli
        if not is_cli:
            print("\nAttempt to login...")
        self.login(u, p)
        self.setTarget(target)
        self.writeFile = is_file
        self.jsonDump = is_json

    def clear_cookies(self, clear_cookies):
        if clear_cookies:
            self.clear_cache()

    def __getstate__(self)
        data = []

        result = self.api.user_feed(str(self.target_id))
        data.extend(result.get('items', []

        return data

    def __get_comments__(self, media_id):
        comments = []

        result = self.api.media_comments(str(media_id))
        comments.extend(result.get('comments', []))

        next_max_id = result.get('next_max_id')
        while next_max_id:
            results = self.api.media_comments(str(media_id), max_id=next_max_id)
            comments.extend(results.get('comments', []))
            next_max_id = results.get('next_max_id')

        return comments

    def __printTargetBanner__(self):
        pc.printout("\nLogged as ", pc.GREEN)
        pc.printout(self.api.username, pc.CYAN)
        pc.printout(". Target: ", pc.GREEN)
        pc.printout(str(self.target), pc.CYAN)
        pc.printout(" [" + str(self.target_id) + "]")

        print('\n')

    def change_target(self):

        line = input()
        self.setTarget(line)
        return
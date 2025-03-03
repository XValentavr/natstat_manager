class NatstatFetchError(Exception):
    def __init__(self, resource: str, sport_code: str) -> None:
        self.sport_code = sport_code
        self.resource = resource
        self.message = f"Received 500 error while fetching {resource} for sport code: {sport_code}"
        super().__init__(self.message)

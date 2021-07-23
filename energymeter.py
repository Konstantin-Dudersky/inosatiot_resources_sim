from influxdb_client import Point


class SimMachine:
    def __init__(self, name):
        self._ep_imp = 0
        self._ep_exp = 0
        self._eq_imp = 0
        self._eq_exp = 0

        self.name = name

        self.ep_imp_increment = 0
        self.ep_exp_increment = 0
        self.eq_imp_increment = 0
        self.eq_exp_increment = 0

    def cycle(self):
        self._ep_imp += self.ep_imp_increment
        self._ep_exp += self.ep_exp_increment
        self._eq_imp += self.eq_imp_increment
        self._eq_exp += self.eq_exp_increment

        return [Point(self.name).field("ep_imp", int(self._ep_imp)),
                Point(self.name).field("ep_exp", int(self._ep_exp)),
                Point(self.name).field("eq_imp", int(self._eq_imp)),
                Point(self.name).field("eq_exp", int(self._eq_exp))]

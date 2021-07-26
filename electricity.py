import datetime
import random

from influxdb_client import Point


class SimElectricity:
    def __init__(self, label, p_base=0, p_var=0, p_delay=0, q_base=0, q_var=0, q_delay=0):
        self.label = label

        self.p_base = float(p_base)
        self.p_var = float(p_var)
        self.p_delay = datetime.timedelta(seconds=p_delay)

        self.q_base = float(q_base)
        self.q_var = float(q_var)
        self.q_delay = datetime.timedelta(seconds=q_delay)

        self._last_exec = datetime.datetime.now()

        self._ep_imp = 0.0
        self._ep_exp = 0.0

        self._eq_imp = 0.0
        self._eq_exp = 0.0

        self._p_current = self.p_base
        self._p_target = self.p_base

        self._q_current = self.q_base
        self._q_target = self.q_base

    def cycle(self):
        # delta time
        current = datetime.datetime.now()
        delta = current - self._last_exec
        self._last_exec = current

        # active power
        if (self._p_target == self.p_base or
                (self.p_base < self._p_target < self._p_current) or
                (self.p_base > self._p_target > self._p_current)):
            self._p_target = self.p_var * (2 * random.random() - 1) + self.p_base

        self._p_current += delta.total_seconds() / self.p_delay.total_seconds() * (self._p_target - self.p_base)

        if self._p_current > 0:
            self._ep_imp += abs(self._p_current) * delta.total_seconds() / 3600
        elif self._p_current < 0:
            self._ep_exp += abs(self._p_current) * delta.total_seconds() / 3600

        # reactive power
        if (self._q_target == self.q_base or
                (self.q_base < self._q_target < self._q_current) or
                (self.q_base > self._q_target > self._q_current)):
            self._q_target = self.q_var * (2 * random.random() - 1) + self.q_base

        self._q_current += delta.total_seconds() / self.p_delay.total_seconds() * (self._q_target - self.q_base)

        if self._q_current > 0:
            self._eq_imp += abs(self._q_current) * delta.total_seconds() / 3600
        elif self._q_current < 0:
            self._eq_exp += abs(self._q_current) * delta.total_seconds() / 3600

        return [
            Point(self.label).field("p", self._p_current),
            Point(self.label).field("q", self._q_current),
            Point(self.label).field("ep_imp", int(self._ep_imp)),
            Point(self.label).field("ep_exp", int(self._ep_exp)),
            Point(self.label).field("eq_imp", int(self._eq_imp)),
            Point(self.label).field("eq_exp", int(self._eq_exp))
        ]

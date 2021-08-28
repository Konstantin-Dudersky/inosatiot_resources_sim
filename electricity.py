import datetime
import math
import random
import sys

from influxdb_client import Point
from loguru import logger

logger.remove()
logger.add(sys.stderr, level='INFO')
logger.add('logs/log.txt', level='INFO', rotation='5 MB')


class SimPoint:

    def __init__(self, base, var, delay):
        self._base = base
        self._var = var
        self._delay = delay

        self._current = self._base
        self._target = self._base
        self._current_delay = datetime.timedelta()

    def cycle(self, delta: datetime.timedelta):
        if (self._target == self._base or self._current_delay >= self._delay or
                (self._base < self._target < self._current) or
                (self._base > self._target > self._current)):
            self._target = self._var * (2 * random.random() - 1) + self._base
            self._current_delay = datetime.timedelta()

        self._current += delta.total_seconds() / self._delay.total_seconds() * (self._target - self._base)
        if self._current > self._base + self._var:
            self._current = self._base + self._var
        elif self._current < self._base - self._var:
            self._current = self._base - self._var

        self._current_delay += delta

    @property
    def value(self):
        return self._current


class SimElectricity:
    def __init__(self, label,
                 i: list, v: list, pf: list, f: list, q_ind: str,
                 now=datetime.datetime.now(), ):
        """

        :type q_ind: 1 - индуктивная нагрузка, 0 - емкостная нагрузка
        """
        self.label = label

        self._last_exec = now

        self._ep_imp = 0.0
        self._ep_exp = 0.0

        self._eq_imp = 0.0
        self._eq_exp = 0.0

        self.points = {
            'f': SimPoint(float(f[0]), float(f[1]), datetime.timedelta(seconds=f[2])),
            'i1': SimPoint(float(i[0]), float(i[1]), datetime.timedelta(seconds=i[2])),
            'i2': SimPoint(float(i[0]), float(i[1]), datetime.timedelta(seconds=i[2])),
            'i3': SimPoint(float(i[0]), float(i[1]), datetime.timedelta(seconds=i[2])),
            'v1': SimPoint(float(v[0]), float(v[1]), datetime.timedelta(seconds=v[2])),
            'v2': SimPoint(float(v[0]), float(v[1]), datetime.timedelta(seconds=v[2])),
            'v3': SimPoint(float(v[0]), float(v[1]), datetime.timedelta(seconds=v[2])),
            'pf1': SimPoint(float(pf[0]), float(pf[1]), datetime.timedelta(seconds=pf[2])),
            'pf2': SimPoint(float(pf[0]), float(pf[1]), datetime.timedelta(seconds=pf[2])),
            'pf3': SimPoint(float(pf[0]), float(pf[1]), datetime.timedelta(seconds=pf[2])),
        }
        self.q_ind = bool(q_ind)

    def cycle(self, now):
        # delta time
        current = now
        delta = current - self._last_exec
        self._last_exec = current

        for key, value in self.points.items():
            self.points[key].cycle(delta)

        f = self.points['f'].value

        i1 = self.points['i1'].value
        i2 = self.points['i2'].value
        i3 = self.points['i3'].value

        v1 = self.points['v1'].value
        v2 = self.points['v2'].value
        v3 = self.points['v3'].value

        pf1 = self.points['pf1'].value
        pf2 = self.points['pf2'].value
        pf3 = self.points['pf3'].value

        p1 = i1 * v1 * pf1
        p2 = i2 * v2 * pf2
        p3 = i3 * v3 * pf3
        p = p1 + p2 + p3
        # print(f'{pf1}, {pf2}, {pf3}')
        q1 = i1 * v1 * math.sin(math.acos(pf1))
        q2 = i2 * v2 * math.sin(math.acos(pf2))
        q3 = i3 * v3 * math.sin(math.acos(pf3))
        if not self.q_ind:
            q1 *= -1
            q2 *= -1
            q3 *= -1
        q = q1 + q2 + q3

        out_float = {
            'f': f,
            'i1': i1,
            'i2': i2,
            'i3': i3,
            'pf1': pf1,
            'pf2': pf2,
            'pf3': pf3,
            'p1': p1,
            'p2': p2,
            'p3': p3,
            'p': p,
            'q1': q1,
            'q2': q2,
            'q3': q3,
            'q': q,
            'v1': v1,
            'v2': v2,
            'v3': v3,
            'v12': math.sqrt(math.pow(v1, 2) + math.pow(v2, 2) - 2 * v1 * v2 * math.cos(2.0944)),
            'v23': math.sqrt(math.pow(v2, 2) + math.pow(v3, 2) - 2 * v2 * v3 * math.cos(2.0944)),
            'v31': math.sqrt(math.pow(v3, 2) + math.pow(v1, 2) - 2 * v3 * v1 * math.cos(2.0944)),
        }

        if p > 0:
            self._ep_imp += abs(p) * delta.total_seconds() / 3600
        elif p < 0:
            self._ep_exp += abs(p) * delta.total_seconds() / 3600

        if q > 0:
            self._eq_imp += abs(q) * delta.total_seconds() / 3600
        elif q < 0:
            self._eq_exp += abs(q) * delta.total_seconds() / 3600

        out_int = {
            'ep_imp': self._ep_imp,
            'ep_exp': self._ep_exp,
            'eq_imp': self._eq_imp,
            'eq_exp': self._eq_exp,
        }

        out_flux = []
        for key, value in out_float.items():
            out_flux.append(
                Point(self.label)
                    .field(key, value)
                    .time(now)
                    .tag('datatype', 'float')
                    .tag('aggfunc', 'max,mean,min')
                    .tag('aggwindow', '')
            )
        for key, value in out_int.items():
            out_flux.append(
                Point(self.label)
                    .field(key, value)
                    .time(now)
                    .tag('datatype', 'int')
                    .tag('aggfunc', 'increase')
                    .tag('aggwindow', '')
            )

        return out_flux

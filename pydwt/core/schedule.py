"""
Module that provide API to create classe that implements schedule
intervals
"""
import datetime
from abc import ABC, abstractmethod
from calendar import Calendar
from dataclasses import dataclass, field


@dataclass
class ScheduleInterface(ABC):
    calendar: Calendar = field(default_factory=Calendar)

    @abstractmethod
    def is_scheduled(self, date: datetime = datetime.datetime.now()):
        """Abstract method to be implemented by child class defining the schedule
        time to run the script.
        """
        raise NotImplementedError


class Daily(ScheduleInterface):
    """Provide a daily implementation of schedule interface"""

    def is_scheduled(self, date: datetime.datetime = datetime.datetime.now()):
        return True


@dataclass
class Weekly(ScheduleInterface):
    """Provide a weekly implementation of schedule interface"""

    weekday: int = 0

    def is_scheduled(self, date: datetime.datetime = datetime.datetime.now()):
        return self.weekday == date.weekday()


@dataclass
class SemiMonthly(ScheduleInterface):
    """Implementation of schedule interface checking if a date is in the
    two first weekday in the month
    """

    weekday: int = 0

    def is_scheduled(self, date: datetime.datetime = datetime.datetime.now()):
        weeksofmonth = self.calendar.monthdatescalendar(date.year, date.month)
        dates_to_check = [
            d
            for week in weeksofmonth
            for d in week
            if d.weekday() == self.weekday and d.month == date.month
        ][::2]
        return date.date() in dates_to_check


@dataclass
class Monthly(ScheduleInterface):
    """Implementation of schedule interface checking if a date is in the
    first weekday in the month
    """

    weekday: int = 0

    def is_scheduled(self, date: datetime.datetime = datetime.datetime.now()):
        weeksofmonth = self.calendar.monthdatescalendar(date.year, date.month)
        date_to_check = [
            d
            for week in weeksofmonth
            for d in week
            if d.weekday() == self.weekday and d.month == date.month
        ][0]
        return date.date() == date_to_check


@dataclass
class MonthlyLastOpenDayInMonth(ScheduleInterface):
    """Implementation of schedule interface checking if a date is the last weekday [0-4]
    of the month.
    """

    def is_scheduled(self, date: datetime.datetime = datetime.datetime.now()):
        weeksofmonth = self.calendar.monthdatescalendar(date.year, date.month)
        last_week = weeksofmonth[-1]
        date_to_check = [
            d for d in last_week if d.weekday() not in [5, 6] and d.month == date.month
        ][-1]
        return date.date() == date_to_check
from pydbt.core.schedule import *
import datetime
import pytest
from calendar import Calendar


@pytest.fixture
def calendar():
    return Calendar()


def test_daily():
    daily = Daily()
    assert daily.is_scheduled() == True


def test_is_sechduled_weekly(calendar):
    date = datetime.datetime.now()
    weeks_of_month = calendar.monthdatescalendar(date.year, date.month)
    week = weeks_of_month[0]
    for date in week:
        weekday = Weekly(weekday=date.weekday())
        assert weekday.is_scheduled(date)


def test_is_not_sechduled_weekly(calendar):
    date = datetime.datetime.now()
    weeks_of_month = calendar.monthdatescalendar(date.year, date.month)
    week = weeks_of_month[0]
    for date in week:
        weekday = Weekly(weekday=date.weekday() + 1)
        assert not weekday.is_scheduled(date)


def test_is_scheduled_monthly(calendar):
    date = datetime.datetime.now()
    weeks_of_month = calendar.monthdatescalendar(date.year, date.month)
    first_week = weeks_of_month[0]
    first_week = [
        d for d in first_week if d.year == date.year and d.month == date.month
    ]
    for date in first_week:
        schedule = Monthly(weekday=date.weekday())
        datet = datetime.datetime(date.year, date.month, date.day)
        assert schedule.is_scheduled(datet)


def test_is_not_scheduled_monthly(calendar):
    date = datetime.datetime.now()
    weeks_of_month = calendar.monthdatescalendar(date.year, date.month)
    not_first_week = weeks_of_month[2]
    not_first_week = [
        d for d in not_first_week if d.year == date.year and d.month == date.month
    ]
    for date in not_first_week:
        schedule = Monthly(weekday=date.weekday())
        datet = datetime.datetime(date.year, date.month, date.day)
        assert not schedule.is_scheduled(datet)


def test_is_schedule_last_open_day_in_month():
    dates = [
        datetime.datetime(2023, 2, 28),
        datetime.datetime(2023, 1, 31),
        datetime.datetime(2022, 12, 30),
        datetime.datetime(2022, 7, 29)
    ]
    s = MonthlyLastOpenDayInMonth()
    for d in dates:
        assert s.is_scheduled(d)


def test_is_not_schedule_last_open_day_in_month():
    dates = [datetime.datetime(2022, 12, 31),datetime.datetime(2022, 7, 31),]
    s = MonthlyLastOpenDayInMonth()
    for d in dates:
        assert not s.is_scheduled(d)

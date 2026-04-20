import csv
import json
import time
from collections import Counter, defaultdict
from typing import Any, Dict, List

import requests

API_URL = ""

GROUPS = [
    "NAME-YY-N",
]

DAY_NAMES = {
    0: "Понедельник",
    1: "Вторник",
    2: "Среда",
    3: "Четверг",
    4: "Пятница",
    5: "Суббота",
}

WEEK_NAMES = {
    0: "Неделя 0",
    1: "Неделя 1",
}


def group_weight(group: str) -> float:
    return 0.7 if "-24-" in group else 1.0


def time_to_minutes(value: str) -> int:
    hh, mm = map(int, value.split(":"))
    return hh * 60 + mm


def fetch_group_schedule(session: requests.Session, group: str, retries: int = 3, timeout: int = 20) -> List[Dict[str, Any]]:
    payload = {
        "type": "group",
        "selector": {
            "group": group
        }
    }

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = session.post(API_URL, json=payload, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict):
                raise RuntimeError(f"Unexpected response type: {type(data).__name__}")

            if data.get("success") is not True:
                raise RuntimeError(data.get("error", "Unknown API error"))

            schedule = data.get("schedule", [])
            if not isinstance(schedule, list):
                raise RuntimeError("Field 'schedule' is not a list")

            return [x for x in schedule if isinstance(x, dict)]

        except Exception as e:
            last_error = e
            if attempt < retries:
                time.sleep(1.5 * attempt)

    raise RuntimeError(f"Не удалось получить расписание для {group}: {last_error}")


def build_last_end_per_day_and_week(lessons: List[Dict[str, Any]]) -> Dict[int, Dict[int, str]]:
    """
    Возвращает:
    {
      week: {
        day: "HH:MM"
      }
    }
    """
    result: Dict[int, Dict[int, str]] = defaultdict(dict)

    for lesson in lessons:
        day = lesson.get("day")
        week = lesson.get("week")
        end_time = lesson.get("ends")

        if day not in DAY_NAMES:
            continue
        if week not in WEEK_NAMES:
            continue
        if not isinstance(end_time, str):
            continue

        current = result[week].get(day)
        if current is None or time_to_minutes(end_time) > time_to_minutes(current):
            result[week][day] = end_time

    return dict(result)


def print_distribution(title: str, distribution: Dict[str, float]) -> None:
    print(title)
    for t, value in sorted(distribution.items(), key=lambda x: time_to_minutes(x[0])):
        print(f"  {t}: {value}")


def main():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "schedule-analyzer/1.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
    })

    per_group_last_end = {}
    failed_groups = []
    empty_groups = []

    # Статистика:
    # 1. По конкретным неделям
    raw_stats_by_week = {
        0: {day: Counter() for day in DAY_NAMES},
        1: {day: Counter() for day in DAY_NAMES},
    }
    weighted_stats_by_week = {
        0: {day: defaultdict(float) for day in DAY_NAMES},
        1: {day: defaultdict(float) for day in DAY_NAMES},
    }

    # 2. Сводно без разделения по неделям
    raw_stats_combined = {day: Counter() for day in DAY_NAMES}
    weighted_stats_combined = {day: defaultdict(float) for day in DAY_NAMES}

    print("=== Сбор расписаний ===")
    for idx, group in enumerate(GROUPS, start=1):
        try:
            lessons = fetch_group_schedule(session, group)

            if not lessons:
                empty_groups.append(group)
                per_group_last_end[group] = {}
                print(f"[{idx}/{len(GROUPS)}] EMPTY {group}")
                continue

            last_end = build_last_end_per_day_and_week(lessons)
            per_group_last_end[group] = last_end
            w = group_weight(group)

            for week, day_map in last_end.items():
                for day, end_time in day_map.items():
                    raw_stats_by_week[week][day][end_time] += 1
                    weighted_stats_by_week[week][day][end_time] += w

                    raw_stats_combined[day][end_time] += 1
                    weighted_stats_combined[day][end_time] += w

            print(f"[{idx}/{len(GROUPS)}] OK {group}")

        except Exception as e:
            failed_groups.append({"group": group, "error": str(e)})
            print(f"[{idx}/{len(GROUPS)}] ERROR {group}: {e}")

    print("\n=== Итог: по неделям ===")
    summary = {
        "by_week": {},
        "combined": {},
    }

    for week in [0, 1]:
        print(f"\n##### {WEEK_NAMES[week]} #####")
        summary["by_week"][str(week)] = {}

        for day in DAY_NAMES:
            day_name = DAY_NAMES[day]
            raw_distribution = dict(sorted(raw_stats_by_week[week][day].items(), key=lambda x: time_to_minutes(x[0])))
            weighted_distribution = dict(sorted(weighted_stats_by_week[week][day].items(), key=lambda x: time_to_minutes(x[0])))

            print(f"\n--- {day_name} ---")
            if not raw_distribution:
                print("Нет данных")
                summary["by_week"][str(week)][str(day)] = {
                    "day_name": day_name,
                    "raw_distribution": {},
                    "weighted_distribution": {},
                    "raw_mode": None,
                    "weighted_mode": None,
                }
                continue

            print_distribution("Обычная статистика:", raw_distribution)
            raw_mode_time, raw_mode_count = raw_stats_by_week[week][day].most_common(1)[0]
            print(f"Чаще всего: {raw_mode_time} ({raw_mode_count} групп)")

            print_distribution("Взвешенная статистика:", {k: round(v, 2) for k, v in weighted_distribution.items()})
            weighted_mode_time, weighted_mode_value = max(weighted_stats_by_week[week][day].items(), key=lambda x: x[1])
            print(f"Чаще всего по весам: {weighted_mode_time} ({weighted_mode_value:.2f})")

            summary["by_week"][str(week)][str(day)] = {
                "day_name": day_name,
                "raw_distribution": raw_distribution,
                "weighted_distribution": {k: round(v, 2) for k, v in weighted_distribution.items()},
                "raw_mode": {
                    "time": raw_mode_time,
                    "count": raw_mode_count,
                },
                "weighted_mode": {
                    "time": weighted_mode_time,
                    "weight_sum": round(weighted_mode_value, 2),
                },
            }

    print("\n=== Итог: сводно по обеим неделям ===")
    for day in DAY_NAMES:
        day_name = DAY_NAMES[day]
        raw_distribution = dict(sorted(raw_stats_combined[day].items(), key=lambda x: time_to_minutes(x[0])))
        weighted_distribution = dict(sorted(weighted_stats_combined[day].items(), key=lambda x: time_to_minutes(x[0])))

        print(f"\n--- {day_name} ---")
        if not raw_distribution:
            print("Нет данных")
            summary["combined"][str(day)] = {
                "day_name": day_name,
                "raw_distribution": {},
                "weighted_distribution": {},
                "raw_mode": None,
                "weighted_mode": None,
            }
            continue

        print_distribution("Обычная статистика:", raw_distribution)
        raw_mode_time, raw_mode_count = raw_stats_combined[day].most_common(1)[0]
        print(f"Чаще всего: {raw_mode_time} ({raw_mode_count} групп)")

        print_distribution("Взвешенная статистика:", {k: round(v, 2) for k, v in weighted_distribution.items()})
        weighted_mode_time, weighted_mode_value = max(weighted_stats_combined[day].items(), key=lambda x: x[1])
        print(f"Чаще всего по весам: {weighted_mode_time} ({weighted_mode_value:.2f})")

        summary["combined"][str(day)] = {
            "day_name": day_name,
            "raw_distribution": raw_distribution,
            "weighted_distribution": {k: round(v, 2) for k, v in weighted_distribution.items()},
            "raw_mode": {
                "time": raw_mode_time,
                "count": raw_mode_count,
            },
            "weighted_mode": {
                "time": weighted_mode_time,
                "weight_sum": round(weighted_mode_value, 2),
            },
        }

    # CSV: по группам
    with open("per_group_last_end.csv", "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["group", "week", "day", "day_name", "last_end"])
        for group, weeks in per_group_last_end.items():
            for week, day_map in weeks.items():
                for day, end_time in sorted(day_map.items()):
                    writer.writerow([group, week, day, DAY_NAMES[day], end_time])

    # CSV: агрегаты по дням и неделям
    with open("weekday_end_stats.csv", "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["scope", "week", "day", "day_name", "time", "raw_count", "weighted_count"])

        for week in [0, 1]:
            for day in DAY_NAMES:
                all_times = sorted(
                    set(raw_stats_by_week[week][day].keys()) | set(weighted_stats_by_week[week][day].keys()),
                    key=time_to_minutes
                )
                for t in all_times:
                    writer.writerow([
                        "by_week",
                        week,
                        day,
                        DAY_NAMES[day],
                        t,
                        raw_stats_by_week[week][day].get(t, 0),
                        round(weighted_stats_by_week[week][day].get(t, 0.0), 2),
                    ])

        for day in DAY_NAMES:
            all_times = sorted(
                set(raw_stats_combined[day].keys()) | set(weighted_stats_combined[day].keys()),
                key=time_to_minutes
            )
            for t in all_times:
                writer.writerow([
                    "combined",
                    "",
                    day,
                    DAY_NAMES[day],
                    t,
                    raw_stats_combined[day].get(t, 0),
                    round(weighted_stats_combined[day].get(t, 0.0), 2),
                ])

    with open("per_group_last_end.json", "w", encoding="utf-8") as f:
        json.dump(per_group_last_end, f, ensure_ascii=False, indent=2)

    with open("weekday_end_stats.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    with open("empty_groups.json", "w", encoding="utf-8") as f:
        json.dump(empty_groups, f, ensure_ascii=False, indent=2)

    if failed_groups:
        with open("failed_groups.json", "w", encoding="utf-8") as f:
            json.dump(failed_groups, f, ensure_ascii=False, indent=2)

    print("\n=== Файлы сохранены ===")
    print("- per_group_last_end.csv")
    print("- weekday_end_stats.csv")
    print("- per_group_last_end.json")
    print("- weekday_end_stats.json")
    print("- empty_groups.json")
    if failed_groups:
        print("- failed_groups.json")


if __name__ == "__main__":
    main()
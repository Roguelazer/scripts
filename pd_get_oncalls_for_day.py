import argparse
import collections
import datetime
import dateutil.parser
import pygerduty
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pagerduty-account', required=True)
    parser.add_argument('-k', '--pagerduty-api-key', required=True)
    parser.add_argument('-s', '--schedule-filter', default=[], action='append')
    parser.add_argument('date', type=dateutil.parser.parse, nargs=1)
    args = parser.parse_args()

    dt = args.date[0]
    since = dt - datetime.timedelta(hours=1)
    until = dt + datetime.timedelta(hours=1)

    conn = pygerduty.PagerDuty(args.pagerduty_account, args.pagerduty_api_key)
    people = collections.defaultdict(set)
    for schedule in conn.schedules.list():
        if args.schedule_filter:
            if not any(f in schedule for f in args.schedule_filter):
                continue
        entries = schedule.entries.list(since=since, until=until)
        service_name = schedule.name.split(' ')[0].lower()
        people[(entries[0].user.name, entries[0].user.email)].add(service_name)
    for (person, email), schedules in sorted(people.items()):
        print '{} <{}> -- [{}]'.format(person, email, ', '.join(sorted(schedules)))


if __name__ == '__main__':
    sys.exit(main())

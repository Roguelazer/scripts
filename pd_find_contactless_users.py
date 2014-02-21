import argparse
import copy
import pygerduty
import sys

REQUIRED_TYPES = frozenset(['SMS', 'phone', 'push_notification'])


def list_paginated(container_class, **kwargs):
    # PD is paginated; pygerduty is not
    result = []
    limit = 25
    offset = 0
    while True:
        this_kwargs = copy.copy(kwargs)
        this_kwargs.update({
            'limit': limit,
            'offset': offset,
        })
        this_list = container_class.list(**this_kwargs)
        if not this_list:
            break
        result.extend(this_list)
        offset += len(this_list)
        if len(this_list) > limit:
            # sometimes pagerduty decides to ignore your limit and just
            # return everything. it seems to only do this when you're near
            # the last page.
            break
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pagerduty-account', required=True)
    parser.add_argument('-k', '--pagerduty-api-key', required=True)
    args = parser.parse_args()

    conn = pygerduty.PagerDuty(args.pagerduty_account, args.pagerduty_api_key)
    users_in_a_schedule = set()

    for schedule in list_paginated(conn.schedules):
        these_users = set(u.email for u in schedule.users.list())
        users_in_a_schedule |= these_users
    for user in list_paginated(conn.users, **{'include[]': 'notification_rules'}):
        types = set(rule.contact_method.type for rule in user.notification_rules.list())
        if user.email not in users_in_a_schedule:
            continue
        if not (types & REQUIRED_TYPES):
            print '%s: %s' % (user.email, ', '.join(sorted(types)))


if __name__ == '__main__':
    sys.exit(main())

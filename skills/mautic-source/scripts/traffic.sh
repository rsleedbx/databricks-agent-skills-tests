#!/usr/bin/env bash
# traffic.sh: Mautic's own writes, as a user's traffic would make them, so the source changes the way the app changes it
# (not the way a generic generator would). Runs on the host with the Mautic containers; one round of:
#   pixel   HITS tracking-pixel page hits (/mtracking.gif: page_hits, lead_event_log, new anonymous contacts)
#   form    FORMS posts of form 1 (/form/submit: form_submissions, form_results_1_*, a contact made or updated)
#   api     CONTACTS contacts created, then edited (leads updated), added to segment 1, then a third of them removed
#           from it and a fifth deleted (the REST API, basic auth)
#   email   template e-mail 1 to the newest OPENS contacts (the API, a null mailer: email_stats inserted), then each one
#           opened (its tracking pixel, /email/<hash>.gif: email_stats updated in place, email_stats_devices)
#   cron    mautic:segments:update, mautic:campaigns:update, mautic:campaigns:trigger, mautic:messages:send,
#           mautic:webhooks:process (the scheduled jobs: segment and campaign membership, campaign events)
# Needs: BASE (the web container's URL, e.g. http://127.0.0.1:18080), AUTH (user:password of a Mautic admin), WEB (the
# web container's name, for the cron commands), PODMAN; optional HITS (20), FORMS (10), CONTACTS (10), SEED (the round:
# makes the names and e-mails unique per round), OPENS (10), STEPS ("pixel form api email cron").
# Prints one line per step: what it sent, and how many requests failed.
set -uo pipefail
: "${BASE:?}" "${AUTH:?}" "${WEB:?}" "${PODMAN:?}"
HITS=${HITS:-20}; FORMS=${FORMS:-10}; CONTACTS=${CONTACTS:-10}; OPENS=${OPENS:-10}; SEED=${SEED:-$(date +%s)}
STEPS=${STEPS:-pixel form api email cron}
# a browser's user agent: Mautic doesn't track requests it takes for a bot (curl's own agent is one)
UA='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'
get() { curl -s -o /dev/null -w '%{http_code}' -A "$UA" "$@"; }
api() { curl -s -u "$AUTH" -H 'Content-Type: application/json' "$@"; }
for step in $STEPS; do case $step in
  pixel)
    bad=0; for i in $(seq 1 "$HITS"); do
      c=$(get "$BASE/mtracking.gif?page_url=https%3A%2F%2Fexample.com%2Fp$((i % 7))&page_title=Page+$((i % 7))&page_referrer=&r$SEED-$i")
      [ "$c" = 200 ] || bad=$((bad + 1)); done
    echo "pixel: $HITS hits, $bad failed";;
  form)
    bad=0; for i in $(seq 1 "$FORMS"); do
      c=$(get -X POST "$BASE/form/submit?formId=1" -F "mauticform[formId]=1" -F "mauticform[return]=" \
        -F "mauticform[name]=Visitor $SEED-$i" -F "mauticform[email]=v$SEED-$i@example.com" -F "mauticform[numberofattendees]=$((i % 5 + 1))")
      case $c in 200|302) ;; *) bad=$((bad + 1));; esac; done
    echo "form: $FORMS posts, $bad failed";;
  api)
    ids=(); bad=0
    for i in $(seq 1 "$CONTACTS"); do
      id=$(api -X POST "$BASE/api/contacts/new" -d "{\"firstname\":\"C$SEED\",\"lastname\":\"N$i\",\"email\":\"c$SEED-$i@example.com\"}" | jq -r '.contact.id // empty')
      [ -n "$id" ] && ids+=("$id") || bad=$((bad + 1)); done
    for id in ${ids[@]+"${ids[@]}"}; do
      api -X PATCH "$BASE/api/contacts/$id/edit" -d "{\"city\":\"City $((id % 9))\",\"points\":$((id % 50))}" | jq -e '.contact.id' >/dev/null || bad=$((bad + 1))
      api -X POST "$BASE/api/segments/1/contact/$id/add" | jq -e '.success' >/dev/null || bad=$((bad + 1)); done
    n=0; for id in ${ids[@]+"${ids[@]}"}; do n=$((n + 1))
      [ $((n % 3)) = 0 ] && { api -X POST "$BASE/api/segments/1/contact/$id/remove" | jq -e '.success' >/dev/null || bad=$((bad + 1)); }
      [ $((n % 5)) = 0 ] && { api -X DELETE "$BASE/api/contacts/$id/delete" | jq -e '.contact' >/dev/null || bad=$((bad + 1)); }; done
    echo "api: ${#ids[@]} contacts created and edited, added to segment 1, $((${#ids[@]} / 3)) removed, $((${#ids[@]} / 5)) deleted; $bad failed";;
  email)
    # template e-mail 1 to the newest OPENS contacts with an e-mail address (null mailer), then each one opened once
    cids=$(api "$BASE/api/contacts?limit=$OPENS&orderBy=id&orderByDir=desc&search=email:%25example.com" | jq -r '.contacts // {} | keys[]')
    bad=0; sent=0; for c in $cids; do api -X POST "$BASE/api/emails/1/contact/$c/send" | jq -e '.success' >/dev/null && sent=$((sent + 1)) || bad=$((bad + 1)); done
    hashes=$(api "$BASE/api/stats/email_stats?limit=1000" | jq -r '[.stats[]? | select((.is_read | tostring) == "0") | .tracking_hash // empty] | .[-'"$OPENS"':][]')
    n=0; for h in $hashes; do n=$((n + 1)); c=$(get "$BASE/email/$h.gif"); [ "$c" = 200 ] || bad=$((bad + 1)); done
    echo "email: $sent sent, $n opened; $bad failed";;
  cron)
    for c in mautic:segments:update mautic:campaigns:update mautic:campaigns:trigger mautic:messages:send mautic:webhooks:process; do
      $PODMAN exec -u www-data "$WEB" php /var/www/html/bin/console "$c" --no-interaction >/dev/null 2>&1 && r=ok || r=failed
      echo "cron: $c $r"; done;;
esac; done

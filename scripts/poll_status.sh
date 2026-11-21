#!/bin/sh
# Poll the public status page for a receipt: scripts/poll_status.sh BB-XXXXXXXX
curl -sL https://www.joinbuilders.org/buildathon/status | python3 -c "
import re,sys
t=sys.stdin.read().replace('\\\\\"','\"')
hits=[m.group(0) for m in re.finditer(r'\{[^{}]*\"receipt\":\"$1\"[^{}]*\}',t)]
print('\n'.join(dict.fromkeys(hits)) or 'not in recent list')
"

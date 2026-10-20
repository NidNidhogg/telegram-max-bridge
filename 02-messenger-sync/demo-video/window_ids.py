"""Print CGWindow IDs for the two test chat applications."""

import Quartz

for window in Quartz.CGWindowListCopyWindowInfo(Quartz.kCGWindowListOptionAll, Quartz.kCGNullWindowID):
    owner = window.get("kCGWindowOwnerName")
    if owner in {"Safari", "Telegram Lite"}:
        print(owner, window.get("kCGWindowNumber"), window.get("kCGWindowName", ""))

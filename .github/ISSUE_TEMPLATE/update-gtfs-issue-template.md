---
name: Update GTFS Issue Template
about: Use this template to track a needed update to an agency's GTFS.
title: "[update] AGENCY_NAME GTFS"
labels: ''
assignees: ''

---

- [ ] Check with agency website and/or contact to confirm if there are any changes to the service
- [ ] Update service dates for `mm/dd/yyyyy` - `mm/dd/yyyy`
    - [ ] `calendar.txt`
    - [ ] `calendar_dates.txt`
    - [ ] `feed_info.txt`
- [ ] Re-zip all files
- [ ] Run new GTFS through the official [MobilityData GTFS validator]https://github.com/MobilityData/gtfs-validator) and then track/fix issues as needed.
- [ ] Update `README.md` with last updated date info

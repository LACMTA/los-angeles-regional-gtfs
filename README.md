# los-angeles-regional-gtfs

Metro is hosting GTFS data on behalf of various regional agencies. Each folder contains the `*.txt` files and a `.zip` with all the files bundled together.  These files were originally put together by Trillium on behalf of [Cal-ITP](https://calitp.org/) and then transferred to Metro.

**As of September 2023**, Cal-ITP is in the process of working with each of these transit agencies toward the goal of each transit agency maintaining their GTFS data as much as they are able to. As such, some of these feeds in this repository may only be useful for historical reference and only available via the commit history.

Here are the zip file permalinks:

* [Alhambra](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/alhambra-ca-us/alhambra-ca-us.zip) - *updated 2/10/23*
* [Arcadia](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/arcadia-ca-us/arcadia-ca-us.zip) - *updated 2/10/23*
  * Arcadia's app vendor, Passio, generates a GTFS but it does not pass the MobilityData validator.
* [Artesia](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/artesia-ca-us/artesia-ca-us.zip) - *updated 2/10/23*
* [Baldwin Park](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/baldwinpark-ca-us/baldwinpark-ca-us.zip/) - *updated 2/10/23*
* [Bellflower](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/bellflower-ca-us/bellflower-ca-us.zip) - *updated 2/10/23*
* [Bell Gardens](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/bellgardens-ca-us/bellgardens-ca-us.zip) - *updated 2/10/23*
* [Calabasas](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/calabasas-ca-us/calabasas-ca-us.zip)
* Catalina Flyer
* [Compton](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/compton-ca-us/compton-ca-us.zip)
* [Cudahy](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/cudahy-ca-us/cudahy-ca-us.zip)
* [Downey](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/downey-ca-us/downey-ca-us.zip)
* El Segundos
* [Get Around Town Express (South Gate)](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/getaroundtownexpress-ca-us/getaroundtownexpress-ca-us.zip)
* [Glendora](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/glendora-ca-us/glendora-ca-us.zip) - Trillium was able to get Glendora into Google Maps.
* [Huntington Park](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/huntingtonpark-ca-us/huntingtonpark-ca-us.zip)
* [La Campana (Bell)](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/lacampana-ca-us/lacampana-ca-us.zip)
* La Puente
* Lynwood
* [Maywood](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/maywood-ca-us/maywood-ca-us.zip)
* ~[Montebello](https://github.com/LACMTA/los-angeles-regional-gtfs/blob/14f09a256ac3f8b999b3ed7e701a21bb63bf5e1d/montebello-ca-us/montebello-ca-us.zip) - This feed is most likely out of date. Montebello currently publishes their feed to Google Maps through Avail.~  *Removed 3/2/2022*
  * Montebello is publishing their GTFS data through their vendor (Avail). Service changes at least 2-3 times a year.
  <br>[GTFS zip file](https://mbl.rideralerts.com/infopoint/gtfs-zip.ashx)
  <br>[GTFS API](https://mbl.rideralerts.com/InfoPoint/swagger/ui/index)
  <br>[GTFS-rt Vehicle Positions](https://mbl.rideralerts.com/InfoPoint/GTFS-Realtime.ashx?Type=VehiclePosition)
  <br>[GTFS-rt Alerts](https://mbl.rideralerts.com/InfoPoint/GTFS-Realtime.ashx?Type=Alert)
* ~[Playa Vista Shuttle](https://github.com/LACMTA/los-angeles-regional-gtfs/blob/1df1e2bc6df9db96b600287bf14fe418e10c84a6/playavistashuttle-ca-us/playavistashuttle-ca-us.zip)~ *Cal-ITP is no longer maintaining this feed as it is not provided by a public entity.*
* [Rosemead](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/rosemead-ca-us/rosemead-ca-us.zip)
* [Sierra Madre](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/sierramadre-ca-us/sierramadre-ca-us.zip)
* [West Covina](https://raw.githubusercontent.com/LACMTA/los-angeles-regional-gtfs/main/westcovina-ca-us/westcovina-ca-us.zip)

## Disclaimer

LA Metro cannot be held liable for any omissions and inaccuracies.

## Python Notes

Run `zip_each_dir.py` to generate zip files for each directory.  Requires Python 3.

# Documentation of TMB_DAO functions

To access the functions, create a new instance of the tmb_dao

<br>

## insert_msg

    insert_msg(ais_json, is_json)

    Inserts a single ais message into the tmb database.

    Parameters:
    ais_json = A single ais message in json format.
    
    is_json = A boolean integer (1, 0) denoting whether the ais_message
    is a string (0) or already a python json object (1)

## insert_message_batch

    insert_msg_batch(batch_ais_json)

    Inserts a batch of json format ais messages.

    Parameters:
    batch_ais_json = a json document of ais messages. can be a string or python json object

## read_position_by_mmsi

    read_position_by_mmsi(mmsi)

    Gets coordinates of the vessel with the given MMSI

    Parameters:
    mmsi = a vessel mmsi

    Returns:
    An array of coordinates, [Latitude, Longitude]

## read_recent_vessel_positions

    read_recent_vessel_positions()

    Get the MMSI and coordinates of every vessel who has logged a position report.

    Returns:
    A 2D array of [MMSI, Latitude, Longitude] for every vessel

## read_vessel_information

    read_vessel_information(mmsi)

    Get the vessel document for the vessel with a given mmsi

    Parameters:
    mmsi = a vessel mmsi

    Returns:
    a vessel document containing IMO, Name, CallSign, etc...

## read_port_by_name

    read_port_by_name(name)

    Get the information about a port with a given name

    Returns:
    A port document containing port information. Nothing if there 
    is no port found.

## read_positions_of_ships_headed_to_port(name)

    read the positions of all ships headed to a given port

    Returns:
    A list of vessel MMSI and their coordinates

## read_positions_of_ships_headed_to_port_id(id)

    the same as the last query except port is searched by id

## delete_ais_older_than_five_minutes(current_time)

    delete all ais messages that are 5 minutes older than the time provided

    Parameters:
    current_time = A string date in the mysql format "YYYY-MM-DD HH:MM:SS"

## read_ship_positions_in_tile(id)

    get positions of ships inside a given tile id

    Returns:
    A list of ship MMSI and coordinates

## read_ship_positions_by_port(port_name)

    get positions of vessels who are contained in the same 3rd level tile as the port with a given name

    Returns:
    A list of MMSi and coordinates

## read_level_3_tiles(id):

    get the 4 child tiles of a given level 2 tile id

    Returns:
    a list with 4 elements

## get_tile_file(id):

    get the file name of the image of a map tile id

    Returns:
    a string filename associated with the tile 

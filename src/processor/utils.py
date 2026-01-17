import math

EARTH_RADIUS_NM = 3440.065

# Haversine distance = the angular distance between two points on a sphere's surface
def haversine_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return EARTH_RADIUS_NM * c

# Closest point of approach; calculate the predicted closest distance & time between two vessels
def calculate_cpa(vessel_a, vessel_b):
    current_distance = haversine_distance(
        vessel_a['lat'], vessel_a['lon'],
        vessel_b['lat'], vessel_b['lon']
    )
    # speed (SOG) = x (East-West motion), direction (COG) = y (North-South motion)
    va_x = vessel_a['sog'] * math.sin(math.radians(vessel_a['cog']))
    va_y = vessel_a['sog'] * math.cos(math.radians(vessel_a['cog']))
    vb_x = vessel_b['sog'] * math.sin(math.radians(vessel_b['cog']))
    vb_y = vessel_b['sog'] * math.cos(math.radians(vessel_b['cog']))
    
    vr_x = va_x - vb_x
    vr_y = va_y - vb_y
    
    dlat_nm = (vessel_a['lat'] - vessel_b['lat']) * 60 # 1 deg latitiude = 60 nm
    dlon_nm = (vessel_a['lon'] - vessel_b['lon']) * 60 * math.cos(math.radians(vessel_a['lat']))
    
    relative_speed = math.sqrt(vr_x**2 + vr_y**2)
    if relative_speed < 0.1: # if stationary
        return current_distance, None
    
    # time until CPA
    t_cpa = -(dlat_nm * vr_y + dlon_nm * vr_x) / (vr_x**2 + vr_y**2)
    if t_cpa < 0:
        return current_distance, None
    
    cpa_dlat = dlat_nm + vr_y * t_cpa
    cpa_dlon = dlon_nm + vr_x * t_cpa
    cpa_distance = math.sqrt(cpa_dlat**2 + cpa_dlon**2)
    
    # return CPA in nm & time until CPA in minutes
    return cpa_distance, t_cpa * 60

def is_collision_risk(cpa_distance, time_to_cpa_minutes, vessel_a, vessel_b):
    if cpa_distance is None or time_to_cpa_minutes is None:
        return False
    
    # only consider moving vessels using navigation status
    if vessel_a.get('nav_status', 15) not in [0, 7, 8] or \
       vessel_b.get('nav_status', 15) not in [0, 7, 8]:
        return False
    
    return cpa_distance < 0.5 and time_to_cpa_minutes < 10
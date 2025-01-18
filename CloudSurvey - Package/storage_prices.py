def get_transfer_cost(region_from, region_to, provider, volume):
    if provider == "Azure":
        return volume * 0.0192 #research shows that for inter eu data transfer it always costs 0.0192 per GB

def get_storage_cost(region, provider, volume):
    if provider == "Azure":
        return volume
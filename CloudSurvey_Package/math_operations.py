import numpy as np
from scipy.stats import t

def calculate_konfidenzintervall(list, konfidenzgrad):
    if len(list) > 1:
        if list[1] == []:
            return [0, 0, 0]
        mean = np.mean(list)  # Mittelwert
        std_dev = np.std(list, ddof=1)  # Standardabweichung (ddof=1 für Stichprobe)
        n = len(list)  # Stichprobengröße
        standard_error = std_dev / np.sqrt(n)  # Standardfehler

        list.sort()
        # Kritischer t-Wert für 95% Konfidenzintervall und df = n-1
        alpha = 1 - konfidenzgrad / 100
        t_value = t.ppf(1 - alpha, df=n - 1)

        # Konfidenzintervall
        lower_bound = max(mean - t_value * standard_error, list[0])
        upper_bound = min(mean + t_value * standard_error, list[len(list)- 1])
    else:
        return [0, 0, 0]

    return [lower_bound, mean, upper_bound]

def second_to_hour(seconds):
    return seconds / 3600

def gb_to_gib(volume):
    return float(volume) / 1.074



use std::collections::{VecDeque, HashMap};

pub struct RollingStatistics {
    is_ready: bool,
    n: f64,
    sum: f64,
    m2: f64,
    window_size: usize,
    data: VecDeque<f64>,
}

impl Default for RollingStatistics {
    fn default() -> Self {
        RollingStatistics {
            is_ready: false,
            n: 0.0,
            sum: 0.0,
            m2: 0.0,
            window_size: 20 * 12 * 5 * 100,
            data: VecDeque::with_capacity(20 * 12 * 5 * 100)
        }
    }
}

impl RollingStatistics {
    pub fn new(
        window_size: usize
    ) -> Self {
        RollingStatistics {
            is_ready: false,
            window_size,
            n: 0.0,
            sum: 0.0,
            m2: 0.0,
            data: VecDeque::with_capacity(window_size),
        }
    }

    fn _update_weighted(&mut self, x: f64, weight: f64) {
        if self.data.len() == 0 {
            self.n = weight;
            self.sum = x * weight;
        } else {
            let val = x * weight;
            let tmp = self.n * val - self.sum * weight;
            let prev_n = self.n;
            self.n += weight;
            self.sum += val;
            self.m2 += tmp * tmp / (weight * self.n * prev_n)
        }
    }

    pub fn update(&mut self, x: f64) {
        if self.data.len() == self.window_size
        {
            self.is_ready = true;
            let left_most_val: Option<f64> = self.data.pop_back();
            self._update_weighted(left_most_val.unwrap(), -1.0);
        }
        self._update_weighted(x, 1.0);
        self.data.push_front(x);
    }

    pub fn standard_deviation(&self) -> f64 {
        if self.is_ready {
            return (self.m2 / (self.n - 1.0)).sqrt();
        }
        return f64::NAN;
    }

    pub fn mean(&self) -> f64 {
        if self.is_ready {
            return self.sum / self.n;
        }
        return f64::NAN;
    }
}

pub struct BinnedRollingStatistics {
    window_size: usize,
    hash_map: HashMap<u16, RollingStatistics>,
}

impl Default for BinnedRollingStatistics {
    fn default() -> Self {
        BinnedRollingStatistics {
            window_size: 20 * 12 * 5 * 100,
            hash_map: HashMap::new(),
        }
    }
}

impl BinnedRollingStatistics {
    pub fn new(
        window_size: usize,
    ) -> Self {
        BinnedRollingStatistics {
            window_size,
            hash_map: HashMap::new(),
        }
    }

    fn _key(hour: u8, minute: u8) -> u16 {
        let bin_size = 5;
        let key: u16 = (hour as u16) << 8 | ((minute / bin_size) as u16);
        return key;
    }

    pub fn update(&mut self, hour: u8, minute: u8, value: f64) {
        let key = BinnedRollingStatistics::_key(hour, minute);
        self.hash_map.entry(key)
            .or_insert(RollingStatistics::new(self.window_size)).update(value);
    }

    pub fn standard_deviation(&mut self, hour: u8, minute: u8) -> f64 {
        let key = BinnedRollingStatistics::_key(hour, minute);

        if self.hash_map.contains_key(&key) {
            return self.hash_map[&key].standard_deviation();
        }
        return 0.;
    }

    pub fn mean(&mut self, hour: u8, minute: u8) -> f64 {
        let key = BinnedRollingStatistics::_key(hour, minute);

        if self.hash_map.contains_key(&key) {
            return self.hash_map[&key].mean();
        }
        return 0.;
    }

    pub fn update_and_return_z_score(&mut self, hour: u8, minute: u8, value: f64) -> f64 {
        self.update(hour, minute, value);

        let sigma: f64 = self.standard_deviation(hour, minute);
        match sigma {
            sigma if sigma.is_nan() => f64::NAN,
            sigma if sigma <= 0.0 && sigma >= 0.0  => 0.0,
            _ => (value - self.mean(hour, minute)) / sigma
        }
    }

}

#[derive(Debug)]
pub struct ThreadSize {
    pub size: usize,
}

pub static mut FILTERX_THREAD_SIZE: usize = 1;

impl ThreadSize {
    pub fn new(n: usize) -> Self {
        ThreadSize { size: n }
    }

    pub fn set_global(&self) {
        unsafe {
            FILTERX_THREAD_SIZE = self.size;
        }
        self.set_polars_threads();
    }

    pub fn update(&mut self, size: usize) {
        self.size = size;
    }

    pub fn set_polars_threads(&self) {
        std::env::set_var("POLARS_MAX_THREADS", self.size.to_string());
    }

    pub fn get() -> usize {
        unsafe { FILTERX_THREAD_SIZE }
    }
}

impl Default for ThreadSize {
    fn default() -> Self {
        ThreadSize::new(num_cpus::get())
    }
}

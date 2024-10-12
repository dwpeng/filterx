pub trait Filter {
    type FilterOptions;

    fn filter(&self, filter_option: &Self::FilterOptions) -> bool;
}

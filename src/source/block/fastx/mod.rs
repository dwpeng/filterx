pub mod fasta;
pub mod fastq;

pub trait FastxRecord: std::fmt::Display {
    fn name(&self) -> &str;
    unsafe fn mut_seq(&mut self) -> &mut str;
    fn seq(&self) -> &str;
    fn comment(&self) -> Option<&str>;
    fn qual(&self) -> Option<&str>;
    fn reverse_complement(&mut self) {
        let sequence: &mut str = unsafe { self.mut_seq() };
        let sequence_bytes = unsafe { sequence.as_bytes_mut() };
        let mut i = 0;
        let mut j = sequence_bytes.len() - 1;
        while i < j {
            let tmp = sequence_bytes[i];
            sequence_bytes[i] = sequence_bytes[j];
            sequence_bytes[j] = tmp;
            i += 1;
            j -= 1;
        }
        for i in 0..sequence_bytes.len() {
            match sequence_bytes[i] {
                b'A' => sequence_bytes[i] = b'T',
                b'T' => sequence_bytes[i] = b'A',
                b'G' => sequence_bytes[i] = b'C',
                b'C' => sequence_bytes[i] = b'G',
                b'a' => sequence_bytes[i] = b't',
                b't' => sequence_bytes[i] = b'a',
                b'g' => sequence_bytes[i] = b'c',
                b'c' => sequence_bytes[i] = b'g',
                b'u' => sequence_bytes[i] = b'a',
                b'U' => sequence_bytes[i] = b'A',
                _ => {}
            }
        }
    }
    fn len(&self) -> usize {
        self.seq().len()
    }

    fn lower(&mut self) {
        unsafe {
            self.mut_seq().make_ascii_uppercase();
        }
    }

    fn upper(&mut self) {
        unsafe {
            self.mut_seq().make_ascii_lowercase();
        }
    }

    fn gc(&self) -> usize {
        let mut gc_count = 0_usize;
        for c in self.seq().chars() {
            match c {
                'c' | 'C' | 'g' | 'G' => {
                    gc_count += 1;
                }
                _ => {}
            }
        }
        gc_count
    }
}

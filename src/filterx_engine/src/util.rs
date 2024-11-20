pub fn check_repeat<T: PartialEq>(names: &Vec<T>) -> bool {
    if names.len() == 0 || names.len() == 1 {
        return false;
    }
    for i in 1..names.len() {
        let name = &names[i];
        for j in 0..i {
            if name == &names[j] {
                return true;
            }
        }
    }
    return false;
}

#[test]
fn test_check_repeat() {
    let mut names: Vec<String> = Vec::new();
    names.push("a".into());
    assert_eq!(check_repeat(&names), false);
    names.push("b".into());
    assert_eq!(check_repeat(&names), false);
    names.push("a".into());
    assert_eq!(check_repeat(&names), true);
}

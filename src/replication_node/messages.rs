use std::sync::Mutex;
use std::collections::HashSet;
pub struct Messages {
    sent: Mutex<HashSet<String>>,
    pending: Mutex<HashSet<String>>,
}

impl Messages {

    pub fn default() -> Messages {
        let sent = Mutex::new(HashSet::new());
        let pending = Mutex::new(HashSet::new());
        Messages{
            sent,
            pending,
        }
    }

    pub async fn remove_pending(&self, id: String) {
        let mut pending = self.pending.lock().unwrap();
        println!("removing pending");
        pending.remove(id.as_str());
        println!("pending {:?}", pending);
    }

    pub async fn add_sent(&self, id: String) {
        let mut sent = self.sent.lock().unwrap();
        sent.insert(id.clone());
        println!("adding sent");
        println!("sent {:?}", sent);
    }
    pub async fn add_pending(&self, id: String) {
        let mut pending = self.pending.lock().unwrap();
        println!("adding pending");
        pending.insert(id);
        println!("pending {:?}", pending);
    }

}
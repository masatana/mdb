use anyhow::Result;

use mdb::buffer::{BufferPool, BufferPoolManager};
use mdb::disk::{DiskManager, PageId};
use mdb::table::SimpleTable;

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let mut table = SimpleTable {
        meta_page_id: PageId(0),
        num_key_elems: 1,
    };
    table.create(&mut bufmgr)?;
    dbg!(&table);
    table.insert(&mut bufmgr, &[b"z", b"Alice", b"Smith"])?;
    table.insert(&mut bufmgr, &[b"x", b"Bob", b"Johnson"])?;
    table.insert(&mut bufmgr, &[b"y", b"Charlie", b"Williams"])?;
    table.insert(&mut bufmgr, &[b"w", b"Dave", b"Miller"])?;
    table.insert(&mut bufmgr, &[b"v", b"Eve", b"Brown"])?;
    table.insert(&mut bufmgr, &[b"1", b"1", b"Brown"])?;
    table.insert(&mut bufmgr, &[b"10", b"2", b"Brown"])?;
    table.insert(&mut bufmgr, &[b"9", b"3", b"Brown"])?;
    bufmgr.flush()?;
    Ok(())
}

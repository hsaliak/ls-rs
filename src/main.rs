use clap::Parser;
use std::fs::{self, Metadata};
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use rayon::prelude::*;

// Global thread-safe caches for user/group lookups
static USER_CACHE: Lazy<DashMap<u32, String>> = Lazy::new(|| DashMap::new());
static GROUP_CACHE: Lazy<DashMap<u32, String>> = Lazy::new(|| DashMap::new());

/// Get user name with caching - thread-safe
fn get_user_name_cached(uid: u32) -> String {
    USER_CACHE.entry(uid).or_insert_with(|| get_user_name(uid)).clone()
}

/// Get group name with caching - thread-safe
fn get_group_name_cached(gid: u32) -> String {
    GROUP_CACHE.entry(gid).or_insert_with(|| get_group_name(gid)).clone()
}

#[derive(Parser, Debug)]
#[command(name = "ls")]
#[command(about = "List directory contents")]
struct Args {
    #[arg(short = 'a', long, help = "Include directory entries whose names begin with a dot")]
    all: bool,

    #[arg(short = 'A', long, help = "List all entries except . and ..")]
    almost_all: bool,

    #[arg(short = 'l', help = "List in long format")]
    long: bool,

    #[arg(short = '1', help = "Force output to be one entry per line")]
    one: bool,

    #[arg(short = 't', help = "Sort by modification time")]
    sort_time: bool,

    #[arg(short = 'S', help = "Sort by file size")]
    sort_size: bool,

    #[arg(short = 'r', help = "Reverse sort order")]
    reverse: bool,

    #[arg(short = 'f', help = "Do not sort, list entries in directory order")]
    no_sort: bool,

    #[arg(short = 'F', help = "Append indicator (/, *, =, @, |) to entries")]
    classify: bool,

    #[arg(short = 'p', help = "Append / to directories")]
    slash: bool,

    #[arg(long = "human-readable", help = "Human readable sizes")]
    human_readable: bool,

    #[arg(short = 'G', help = "Enable colorized output")]
    color_flag: bool,

    #[arg(long = "color", value_name = "WHEN", help = "Color mode: auto, always, never")]
    color_when: Option<String>,

    #[arg(short = 'i', long, help = "Print inode")]
    inode: bool,

    #[arg(short = 's', long, help = "Print block count")]
    blocks: bool,

    #[arg(short = 'R', long, help = "Recursively list subdirectories")]
    recursive: bool,

    #[arg(short = 'L', help = "Follow all symlinks to final target")]
    follow_symlinks: bool,

    #[arg(short = 'P', help = "Never follow symlinks")]
    no_follow_symlinks: bool,

    #[arg(short = 'H', help = "Follow symlinks on command line only")]
    follow_cli_symlinks: bool,

    #[arg(short = 'c', help = "Use status change time for sorting")]
    ctime: bool,

    #[arg(short = 'u', help = "Use access time for sorting")]
    atime: bool,

    #[arg(short = 'U', help = "Use creation time for sorting")]
    birthtime: bool,

    #[arg(short = 'C', help = "Force multi-column output (down columns)")]
    multi_column_down: bool,

    #[arg(short = 'x', help = "Force multi-column output (across columns)")]
    multi_column_across: bool,

    #[arg(short = 'm', help = "Stream format (comma-separated)")]
    stream_format: bool,

    #[arg(default_value = ".")]
    paths: Vec<PathBuf>,
}

struct Entry {
    name: String,
    path: PathBuf,
    metadata: Metadata,
    is_symlink: bool,
    symlink_target: Option<PathBuf>,
}

#[derive(Debug)]
struct Config {
    all: bool,
    almost_all: bool,
    long: bool,
    one: bool,
    sort: SortBy,
    reverse: bool,
    classify: bool,
    slash: bool,
    human_readable: bool,
    color: ColorMode,
    inode: bool,
    blocks: bool,
    recursive: bool,
    follow_symlinks: FollowSymlinks,
    time_field: TimeField,
    format: OutputFormat,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum OutputFormat {
    Default,
    MultiColumnDown,
    MultiColumnAcross,
    Stream,
}

#[derive(Debug, Clone, Copy)]
enum FollowSymlinks {
    Never,
    CommandLine,
    Always,
}

#[derive(Debug, Clone, Copy)]
enum TimeField {
    Modify,
    Change,
    Access,
    Birth,
}

#[derive(Debug, Clone, Copy)]
enum SortBy {
    Name,
    Time,
    Size,
    Unsorted,
}

#[derive(Debug, Clone, Copy)]
enum ColorMode {
    Auto,
    Always,
    Never,
}

fn main() {
    let args = Args::parse();
    
    // Determine color mode
    let color = match args.color_when.as_deref() {
        Some("always") => ColorMode::Always,
        Some("never") => ColorMode::Never,
        Some("auto") => ColorMode::Auto,
        _ => if args.color_flag { ColorMode::Always } else { ColorMode::Auto },
    };
    
    // Determine sort order
    let sort = if args.no_sort {
        SortBy::Unsorted
    } else if args.sort_time {
        SortBy::Time
    } else if args.sort_size {
        SortBy::Size
    } else {
        SortBy::Name
    };

    // Determine symlink following behavior
    let follow_symlinks = if args.no_follow_symlinks {
        FollowSymlinks::Never
    } else if args.follow_symlinks {
        FollowSymlinks::Always
    } else if args.follow_cli_symlinks {
        FollowSymlinks::CommandLine
    } else {
        FollowSymlinks::Never  // default for ls -l
    };

    // Determine time field for sorting/display
    let time_field = if args.ctime {
        TimeField::Change
    } else if args.atime {
        TimeField::Access
    } else if args.birthtime {
        TimeField::Birth
    } else {
        TimeField::Modify
    };

    // Determine output format (last specified wins)
    let format = if args.stream_format {
        OutputFormat::Stream
    } else if args.multi_column_across {
        OutputFormat::MultiColumnAcross
    } else if args.multi_column_down {
        OutputFormat::MultiColumnDown
    } else {
        OutputFormat::Default
    };

    let config = Config {
        all: args.all || args.no_sort,
        almost_all: args.almost_all,
        long: args.long,
        one: args.one,
        sort,
        reverse: args.reverse,
        classify: args.classify,
        slash: args.slash,
        human_readable: args.human_readable,
        color,
        inode: args.inode,
        blocks: args.blocks,
        recursive: args.recursive,
        follow_symlinks,
        time_field,
        format,
    };

    let paths = if args.paths.is_empty() {
        vec![PathBuf::from(".")]
    } else {
        args.paths
    };

    let mut stdout = io::stdout();
    let mut first = true;

    for path in &paths {
        if paths.len() > 1 {
            if !first {
                writeln!(stdout).unwrap();
            }
            writeln!(stdout, "{}:", path.display()).unwrap();
            first = false;
        }

        if let Err(e) = list_directory(&path, &config, &mut stdout) {
            eprintln!("ls: {}: {}", path.display(), e);
        }
    }
}

fn list_directory(path: &Path, config: &Config, stdout: &mut dyn Write) -> io::Result<()> {
    let mut entries = collect_entries(path, config)?;
    
    // Apply sorting (use parallel sort for large directories)
    const PARALLEL_SORT_THRESHOLD: usize = 1000;
    
    match config.sort {
        SortBy::Name => {
            if entries.len() > PARALLEL_SORT_THRESHOLD {
                entries.par_sort_by(|a, b| {
                    let cmp = a.name.to_lowercase().cmp(&b.name.to_lowercase());
                    if config.reverse { cmp.reverse() } else { cmp }
                });
            } else {
                entries.sort_by(|a, b| {
                    let cmp = a.name.to_lowercase().cmp(&b.name.to_lowercase());
                    if config.reverse { cmp.reverse() } else { cmp }
                });
            }
        }
        SortBy::Time => {
            if entries.len() > PARALLEL_SORT_THRESHOLD {
                entries.par_sort_by(|a, b| {
                    let a_time = get_time_field(&a.metadata, config.time_field);
                    let b_time = get_time_field(&b.metadata, config.time_field);
                    let cmp = a_time.cmp(&b_time).reverse(); // newest first
                    if cmp == std::cmp::Ordering::Equal {
                        let name_cmp = a.name.to_lowercase().cmp(&b.name.to_lowercase());
                        if config.reverse { name_cmp.reverse() } else { name_cmp }
                    } else if config.reverse {
                        cmp.reverse()
                    } else {
                        cmp
                    }
                });
            } else {
                entries.sort_by(|a, b| {
                    let a_time = get_time_field(&a.metadata, config.time_field);
                    let b_time = get_time_field(&b.metadata, config.time_field);
                    let cmp = a_time.cmp(&b_time).reverse(); // newest first
                    if cmp == std::cmp::Ordering::Equal {
                        let name_cmp = a.name.to_lowercase().cmp(&b.name.to_lowercase());
                        if config.reverse { name_cmp.reverse() } else { name_cmp }
                    } else if config.reverse {
                        cmp.reverse()
                    } else {
                        cmp
                    }
                });
            }
        }
        SortBy::Size => {
            if entries.len() > PARALLEL_SORT_THRESHOLD {
                entries.par_sort_by(|a, b| {
                    let a_size = a.metadata.len();
                    let b_size = b.metadata.len();
                    let cmp = a_size.cmp(&b_size).reverse(); // largest first
                    if cmp == std::cmp::Ordering::Equal {
                        let name_cmp = a.name.to_lowercase().cmp(&b.name.to_lowercase());
                        if config.reverse { name_cmp.reverse() } else { name_cmp }
                    } else if config.reverse {
                        cmp.reverse()
                    } else {
                        cmp
                    }
                });
            } else {
                entries.sort_by(|a, b| {
                    let a_size = a.metadata.len();
                    let b_size = b.metadata.len();
                    let cmp = a_size.cmp(&b_size).reverse(); // largest first
                    if cmp == std::cmp::Ordering::Equal {
                        let name_cmp = a.name.to_lowercase().cmp(&b.name.to_lowercase());
                        if config.reverse { name_cmp.reverse() } else { name_cmp }
                    } else if config.reverse {
                        cmp.reverse()
                    } else {
                        cmp
                    }
                });
            }
        }
        SortBy::Unsorted => {}
    }

    let use_color = match config.color {
        ColorMode::Always => true,
        ColorMode::Never => false,
        ColorMode::Auto => is_tty(),
    };

    // Determine output format
    if config.long {
        print_long_format(&entries, config, stdout, use_color)?;
    } else if config.one {
        print_single_column(&entries, config, stdout, use_color)?;
    } else if config.format == OutputFormat::Stream {
        print_stream_format(&entries, config, stdout, use_color)?;
    } else if config.format == OutputFormat::MultiColumnAcross {
        print_multi_column_across(&entries, config, stdout, use_color)?;
    } else if config.format == OutputFormat::MultiColumnDown || is_tty() {
        print_multi_column_down(&entries, config, stdout, use_color)?;
    } else {
        print_single_column(&entries, config, stdout, use_color)?;
    }

    // Handle recursion
    if config.recursive {
        for entry in entries {
            if entry.metadata.is_dir() {
                writeln!(stdout)?;
                writeln!(stdout, "{}:", entry.path.display())?;
                if let Err(e) = list_directory(&entry.path, config, stdout) {
                    eprintln!("ls: {}: {}", entry.path.display(), e);
                }
            }
        }
    }

    Ok(())
}

fn collect_entries(path: &Path, config: &Config) -> io::Result<Vec<Entry>> {
    // Handle single file case (no parallelism needed)
    if path.is_file() || path.is_symlink() {
        let metadata = fs::symlink_metadata(path)?;
        let name = path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());
        let is_symlink = metadata.file_type().is_symlink();
        let symlink_target = if is_symlink {
            fs::read_link(path).ok()
        } else {
            None
        };
        
        return Ok(vec![Entry {
            name,
            path: path.to_path_buf(),
            metadata,
            is_symlink,
            symlink_target,
        }]);
    }

    // Collect directory entries first (read_dir is sequential)
    let dir_entries: Vec<_> = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    
    // Prepare entry data without metadata
    let entry_data: Vec<_> = dir_entries
        .into_iter()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            
            // Filter dotfiles based on flags
            if name.starts_with('.') {
                if config.all {
                    // -a: show everything including . and ..
                } else if config.almost_all {
                    // -A: show dotfiles except . and ..
                    if name == "." || name == ".." {
                        return None;
                    }
                } else {
                    // default: hide all dotfiles
                    return None;
                }
            }
            
            Some((name, entry.path()))
        })
        .collect();
    
    // Parallel stat calls using rayon
    let entries: Vec<Entry> = entry_data
        .into_par_iter()
        .filter_map(|(name, path)| {
            let metadata = fs::symlink_metadata(&path).ok()?;
            let is_symlink = metadata.file_type().is_symlink();
            let symlink_target = if is_symlink {
                fs::read_link(&path).ok()
            } else {
                None
            };
            
            Some(Entry {
                name,
                path,
                metadata,
                is_symlink,
                symlink_target,
            })
        })
        .collect();

    Ok(entries)
}

fn print_single_column(entries: &[Entry], config: &Config, stdout: &mut dyn Write, use_color: bool) -> io::Result<()> {
    for entry in entries {
        let mut name = entry.name.clone();
        
        if config.classify || config.slash {
            name.push_str(&get_indicator(&entry.metadata, config.classify));
        }
        
        if use_color {
            name = colorize(&name, &entry.metadata);
        }
        
        writeln!(stdout, "{}", name)?;
    }
    Ok(())
}

fn print_multi_column_down(entries: &[Entry], config: &Config, stdout: &mut dyn Write, use_color: bool) -> io::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let mut names: Vec<String> = entries.iter().map(|e| {
        let mut name = e.name.clone();
        if config.classify || config.slash {
            name.push_str(&get_indicator(&e.metadata, config.classify));
        }
        if use_color {
            name = colorize(&name, &e.metadata);
        }
        name
    }).collect();

    let max_len = names.iter().map(|n| n.len()).max().unwrap_or(0);
    let col_width = max_len + 2;
    
    let term_width = terminal_size().unwrap_or(80);
    let num_cols = (term_width / col_width).max(1);
    let num_rows = (entries.len() + num_cols - 1) / num_cols;

    // Print down columns
    for row in 0..num_rows {
        for col in 0..num_cols {
            let idx = col * num_rows + row;
            if idx < entries.len() {
                let name = &names[idx];
                write!(stdout, "{:<width$}", name, width = col_width)?;
            }
        }
        writeln!(stdout)?;
    }

    Ok(())
}

fn print_multi_column_across(entries: &[Entry], config: &Config, stdout: &mut dyn Write, use_color: bool) -> io::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let mut names: Vec<String> = entries.iter().map(|e| {
        let mut name = e.name.clone();
        if config.classify || config.slash {
            name.push_str(&get_indicator(&e.metadata, config.classify));
        }
        if use_color {
            name = colorize(&name, &e.metadata);
        }
        name
    }).collect();

    let max_len = names.iter().map(|n| n.len()).max().unwrap_or(0);
    let col_width = max_len + 2;
    
    let term_width = terminal_size().unwrap_or(80);
    let num_cols = (term_width / col_width).max(1);

    // Print across columns
    for (idx, name) in names.iter().enumerate() {
        write!(stdout, "{:<width$}", name, width = col_width)?;
        if (idx + 1) % num_cols == 0 {
            writeln!(stdout)?;
        }
    }
    if entries.len() % num_cols != 0 {
        writeln!(stdout)?;
    }

    Ok(())
}

fn print_stream_format(entries: &[Entry], config: &Config, stdout: &mut dyn Write, use_color: bool) -> io::Result<()> {
    let mut first = true;
    for entry in entries {
        if !first {
            write!(stdout, ", ")?;
        }
        first = false;
        
        let mut name = entry.name.clone();
        if config.classify || config.slash {
            name.push_str(&get_indicator(&entry.metadata, config.classify));
        }
        if use_color {
            name = colorize(&name, &entry.metadata);
        }
        write!(stdout, "{}", name)?;
    }
    writeln!(stdout)?;
    Ok(())
}

fn print_long_format(entries: &[Entry], config: &Config, stdout: &mut dyn Write, use_color: bool) -> io::Result<()> {
    // Pre-populate caches in parallel for large directories
    if entries.len() > 100 {
        let uids: Vec<_> = entries.iter().map(|e| e.metadata.uid()).collect();
        let gids: Vec<_> = entries.iter().map(|e| e.metadata.gid()).collect();
        
        uids.par_iter().for_each(|&uid| { get_user_name_cached(uid); });
        gids.par_iter().for_each(|&gid| { get_group_name_cached(gid); });
    }

    // Calculate column widths
    let max_size_width = entries.iter()
        .map(|e| format_size(e.metadata.len(), config.human_readable).len())
        .max()
        .unwrap_or(0);
    let max_link_width = entries.iter()
        .map(|e| e.metadata.nlink().to_string().len())
        .max()
        .unwrap_or(0);
    let max_inode_width = if config.inode {
        entries.iter().map(|e| e.metadata.ino().to_string().len()).max().unwrap_or(0)
    } else { 0 };
    let max_blocks_width = if config.blocks {
        entries.iter().map(|e| e.metadata.blocks().to_string().len()).max().unwrap_or(0)
    } else { 0 };

    for entry in entries {
        let mode_str = format_mode(entry.metadata.mode());
        let nlink = entry.metadata.nlink();
        let uid = entry.metadata.uid();
        let gid = entry.metadata.gid();
        let time_val = get_time_field(&entry.metadata, config.time_field);
        let inode = entry.metadata.ino();
        let blocks = entry.metadata.blocks();

        // Check if device file (block or char)
        let file_type = entry.metadata.mode() & 0o170000;
        let is_device = file_type == 0o020000 || file_type == 0o060000;
        
        // Format size or device major:minor
        let size_or_device = if is_device {
            // Extract major and minor device numbers
            let dev = entry.metadata.rdev();
            let major = ((dev >> 24) & 0xFF) as u32;
            let minor = (dev & 0xFFFFFF) as u32;
            format!("{}, {}", major, minor)
        } else {
            format_size(entry.metadata.len(), config.human_readable)
        };

        let user = get_user_name_cached(uid);
        let group = get_group_name_cached(gid);

        let time_str = format_time(time_val);

        // Print inode if requested
        if config.inode {
            write!(stdout, "{:>inode_width$} ", inode, inode_width = max_inode_width)?;
        }

        // Print blocks if requested
        if config.blocks {
            write!(stdout, "{:>blocks_width$} ", blocks, blocks_width = max_blocks_width)?;
        }

        write!(
            stdout,
            "{} {:>link_width$} {:>8} {:>8} {:>size_width$} {} ",
            mode_str,
            nlink,
            user,
            group,
            size_or_device,
            time_str,
            link_width = max_link_width,
            size_width = max_size_width
        )?;

        let mut name = entry.name.clone();
        if config.classify || config.slash {
            name.push_str(&get_indicator(&entry.metadata, config.classify));
        }
        if use_color {
            name = colorize(&name, &entry.metadata);
        }
        write!(stdout, "{}", name)?;

        if let Some(ref target) = entry.symlink_target {
            write!(stdout, " -> {}", target.display())?;
        }

        writeln!(stdout)?;
    }

    Ok(())
}

fn format_mode(mode: u32) -> String {
    let file_type = match mode & 0o170000 {
        0o040000 => 'd',
        0o120000 => 'l',
        0o020000 => 'c',
        0o060000 => 'b',
        0o010000 => 'p',
        0o140000 => 's',
        _ => '-',
    };

    let perms = [
        (0o400, 'r'), (0o200, 'w'), (0o100, 'x'),
        (0o040, 'r'), (0o020, 'w'), (0o010, 'x'),
        (0o004, 'r'), (0o002, 'w'), (0o001, 'x'),
    ];

    let mut result = String::with_capacity(10);
    result.push(file_type);
    
    for (bit, ch) in perms {
        if mode & bit != 0 {
            result.push(ch);
        } else {
            result.push('-');
        }
    }

    result
}

fn format_time(mtime: i64) -> String {
    use std::time::{SystemTime, Duration};
    
    let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(mtime as u64);
    let now = SystemTime::now();
    let six_months = Duration::from_secs(6 * 30 * 24 * 60 * 60);
    
    let show_year = if let Ok(diff) = now.duration_since(mtime) {
        diff > six_months
    } else {
        true
    };

    let datetime: chrono::DateTime<chrono::Local> = mtime.into();
    
    if show_year {
        datetime.format("%b %e  %Y").to_string()
    } else {
        datetime.format("%b %e %H:%M").to_string()
    }
}

fn get_user_name(uid: u32) -> String {
    unsafe {
        let pw = libc::getpwuid(uid);
        if pw.is_null() {
            uid.to_string()
        } else {
            let name = std::ffi::CStr::from_ptr((*pw).pw_name)
                .to_string_lossy()
                .to_string();
            name
        }
    }
}

fn get_group_name(gid: u32) -> String {
    unsafe {
        let gr = libc::getgrgid(gid);
        if gr.is_null() {
            gid.to_string()
        } else {
            let name = std::ffi::CStr::from_ptr((*gr).gr_name)
                .to_string_lossy()
                .to_string();
            name
        }
    }
}

fn get_time_field(metadata: &Metadata, field: TimeField) -> i64 {
    match field {
        TimeField::Modify => metadata.mtime(),
        TimeField::Change => metadata.ctime(),
        TimeField::Access => metadata.atime(),
        TimeField::Birth => metadata.ctime(), // Fallback to ctime if birth not available
    }
}

fn is_tty() -> bool {
    unsafe { libc::isatty(1) == 1 }
}

fn get_indicator(metadata: &Metadata, classify: bool) -> String {
    let mode = metadata.mode();
    let file_type = mode & 0o170000;
    
    if file_type == 0o040000 {
        "/".to_string()
    } else if file_type == 0o120000 {
        if classify { "@".to_string() } else { "".to_string() }
    } else if file_type == 0o140000 {
        if classify { "=".to_string() } else { "".to_string() }
    } else if file_type == 0o010000 {
        if classify { "|".to_string() } else { "".to_string() }
    } else if mode & 0o111 != 0 {
        if classify { "*".to_string() } else { "".to_string() }
    } else {
        "".to_string()
    }
}

fn format_size(size: u64, human_readable: bool) -> String {
    if !human_readable {
        return size.to_string();
    }
    
    const UNITS: &[&str] = &["B", "K", "M", "G", "T", "P"];
    if size == 0 {
        return "0B".to_string();
    }
    
    let mut size_f = size as f64;
    let mut unit_idx = 0;
    
    while size_f >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size_f /= 1024.0;
        unit_idx += 1;
    }
    
    if unit_idx == 0 {
        format!("{}{}", size, UNITS[unit_idx])
    } else if size_f >= 10.0 {
        format!("{:.0}{}", size_f, UNITS[unit_idx])
    } else {
        format!("{:.1}{}", size_f, UNITS[unit_idx])
    }
}

fn colorize(name: &str, metadata: &Metadata) -> String {
    let mode = metadata.mode();
    let file_type = mode & 0o170000;
    
    let color_code = if file_type == 0o040000 {
        "\x1b[34m" // blue for directories
    } else if file_type == 0o120000 {
        "\x1b[36m" // cyan for symlinks
    } else if mode & 0o111 != 0 {
        "\x1b[32m" // green for executables
    } else {
        return name.to_string(); // no color needed
    };
    
    format!("{}{}\x1b[0m", color_code, name)
}

fn terminal_size() -> Option<usize> {
    unsafe {
        let mut winsize: libc::winsize = std::mem::zeroed();
        if libc::ioctl(1, libc::TIOCGWINSZ, &mut winsize) == 0 {
            Some(winsize.ws_col as usize)
        } else {
            None
        }
    }
}

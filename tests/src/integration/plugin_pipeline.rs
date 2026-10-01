// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Plugin task-pipeline integration test.
//!
//! Modeled on the `ImgCompression` sample plugin (see `Thryve TestLib/plugins`):
//! a plugin entry point that fans out into per-item worker tasks, each of which
//! does real I/O-bound work and reports completion, with the last one signaling
//! `Fin` to shut the engine down. This is the shape real Thryve plugins take -
//! unlike the unit tests, nothing here is mocked: a real `Plugin`/`PlgLogic` is
//! loaded into a real `Engine` and driven through real image files on disk.
//!
//! Self-contained on purpose: it generates its own tiny synthetic images into a
//! temp directory rather than depending on the external "Thryve TestLib"
//! sandbox (which hardcodes absolute paths and isn't part of this repo), so it
//! runs unmodified on any machine or CI runner.

mod tests_plugin_pipeline {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use image::{ImageBuffer, Rgb};

    use thryve_run::run::run_interactive::{run_interactive, summon_requirements};
    use thryve_utils::bases::plugin::Plugin;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::extras::jobs::TaskJob;
    use thryve_utils::impl_as_any;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::res::TaskResult;
    use thryve_utils::traits::plugin_logic::PlgLogic;

    /// Real plugin core logic: on load, scans `input_dir` and fans out one
    /// "compress" task per image, exactly like `ImgCompression::initial_obs_job`
    /// -> `load_img` does.
    struct ImgPipelinePlugin {
        input_dir: PathBuf,
        output_dir: PathBuf,
    }

    impl PlgLogic for ImgPipelinePlugin {
        impl_as_any!(ImgPipelinePlugin);
        fn clean_up(&self) {}
    }

    impl ImgPipelinePlugin {
        #[allow(clippy::new_ret_no_self)] // factory returns the Plugin wrapper by design
        fn new(tx: PacketHandle, input_dir: PathBuf, output_dir: PathBuf) -> Plugin {
            let core = Arc::new(Self {
                input_dir,
                output_dir,
            });
            let core_clone = Arc::clone(&core);
            let tx_clone = tx.clone();
            let entry = TaskJob::new(move || core_clone.load_images(tx_clone));

            Plugin::new("Test", core, tx, entry).unwrap()
        }

        fn load_images(&self, tx: PacketHandle) -> TaskResult {
            let mut entries: Vec<PathBuf> = fs::read_dir(&self.input_dir)
                .expect("test fixture input dir should exist")
                .filter_map(|e| e.ok().map(|e| e.path()))
                .collect();
            entries.sort();

            let total = entries.len();
            assert!(
                total > 0,
                "test fixture should have generated at least one image"
            );
            let remaining = Arc::new(AtomicUsize::new(total));
            let output_dir = self.output_dir.clone();

            for path in entries {
                let output_dir = output_dir.clone();
                let remaining = Arc::clone(&remaining);
                let job = TaskJob::new(move || compress_one(&path, &output_dir, &remaining));
                tx.send_task(Task::new("CompressImage", 1, job).unwrap())
                    .unwrap();
            }

            TaskResult::new("Ok", format!("queued {} images", total), "Main")
        }
    }

    /// Real work: downsample one image by averaging 2x2 blocks (same algorithm
    /// as `ImgCompression::process_img`), write it to `output_dir`, and once
    /// every image has been processed, return a `Fin` result so the engine
    /// shuts down - exactly how a real plugin signals it's done.
    fn compress_one(path: &Path, output_dir: &Path, remaining: &Arc<AtomicUsize>) -> TaskResult {
        let img = image::open(path)
            .unwrap_or_else(|e| {
                panic!(
                    "test fixture image {} should be readable: {e}",
                    path.display()
                )
            })
            .to_rgb8();
        let (width, height) = (img.width(), img.height());
        let (new_width, new_height) = (width / 2, height / 2);

        let mut out = ImageBuffer::new(new_width, new_height);
        for y in 0..new_height {
            for x in 0..new_width {
                let (px, py) = (x * 2, y * 2);
                let p1 = img.get_pixel(px, py).0;
                let p2 = img.get_pixel(px + 1, py).0;
                let p3 = img.get_pixel(px, py + 1).0;
                let p4 = img.get_pixel(px + 1, py + 1).0;
                let avg = |i: usize| {
                    ((p1[i] as u16 + p2[i] as u16 + p3[i] as u16 + p4[i] as u16) / 4) as u8
                };
                out.put_pixel(x, y, Rgb([avg(0), avg(1), avg(2)]));
            }
        }

        let file_name = path
            .file_name()
            .expect("fixture image path should have a file name");
        out.save(output_dir.join(file_name))
            .unwrap_or_else(|e| panic!("failed to save compressed image: {e}"));

        if remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
            return TaskResult::fin();
        }

        TaskResult::new("Ok", "compressed one image".to_string(), "Main")
    }

    fn make_synthetic_image(path: &Path, width: u32, height: u32) {
        let img: ImageBuffer<Rgb<u8>, Vec<u8>> = ImageBuffer::from_fn(width, height, |x, y| {
            Rgb([(x * 16 % 256) as u8, (y * 16 % 256) as u8, 128])
        });
        img.save(path)
            .expect("should be able to write synthetic test image");
    }

    #[test]
    fn test_image_pipeline_plugin_processes_all_images_end_to_end() {
        // Arrange - a hermetic temp workspace with a couple of tiny synthetic
        // images, so this test needs nothing outside the repo and is fast.
        let workspace = std::env::temp_dir().join(format!(
            "thryve_it_img_pipeline_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        let input_dir = workspace.join("input");
        let output_dir = workspace.join("output");
        fs::create_dir_all(&input_dir).expect("should create input dir");
        fs::create_dir_all(&output_dir).expect("should create output dir");

        let image_names = ["a.png", "b.png", "c.png"];
        for name in &image_names {
            make_synthetic_image(&input_dir.join(name), 8, 8);
        }

        let config = Config::default();
        let mut ctx = summon_requirements(config);
        ctx.plugins.push(ImgPipelinePlugin::new(
            ctx.packet_handle(),
            input_dir.clone(),
            output_dir.clone(),
        ));

        // See engine_lifecycle.rs - init_panic_hook() installs a process-wide
        // panic hook; contain it around the engine run so other tests aren't
        // affected.
        let prev_hook = std::panic::take_hook();

        // Act - run the real engine end-to-end. The plugin's entry point is
        // sent as an init task automatically by PluginManager::load_plugin.
        let handle = run_interactive(ctx).expect("run_interactive should return a handle");
        handle
            .join()
            .expect("engine should exit cleanly once every image has been compressed");

        std::panic::set_hook(prev_hook);

        // Assert - every input image was actually read, downsampled, and
        // written to disk by real task executions on the real thread pool.
        for name in &image_names {
            let out_path = output_dir.join(name);
            assert!(out_path.exists(), "expected compressed output for {name}");

            let out_img = image::open(&out_path).unwrap();
            assert_eq!(out_img.width(), 4, "compressed width should be halved");
            assert_eq!(out_img.height(), 4, "compressed height should be halved");
        }

        // Cleanup - this is a temp workspace we created, not a permanent artifact.
        let _ = fs::remove_dir_all(&workspace);
    }
}

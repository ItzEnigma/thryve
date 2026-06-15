mod tests_run_interactive {
    use thryve_run::run::run_interactive::summon_requirements;
    use thryve_utils::cfg::config::Config;

    #[test]
    fn test_summon_requirements_creates_ctx() {
        // Arrange
        let config = Config::default();

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert!(ctx.config.service.can_stop);
        assert!(ctx.config.service.can_pause);
    }

    #[test]
    fn test_summon_requirements_with_custom_config() {
        // Arrange
        let mut config = Config::default();
        config.pipes.task_pipe_capacity = 100;
        config.pipes.notif_pipe_capacity = 200;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 100);
        assert_eq!(ctx.config.pipes.notif_pipe_capacity, 200);
    }

    #[test]
    fn test_summon_requirements_with_disabled_service_features() {
        // Arrange
        let mut config = Config::default();
        config.service.can_stop = false;
        config.service.can_pause = false;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert!(!ctx.config.service.can_stop);
        assert!(!ctx.config.service.can_pause);
    }

    #[test]
    fn test_summon_requirements_with_zero_pipe_capacity() {
        // Arrange
        let mut config = Config::default();
        config.pipes.task_pipe_capacity = 0;
        config.pipes.notif_pipe_capacity = 0;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 0);
        assert_eq!(ctx.config.pipes.notif_pipe_capacity, 0);
    }

    #[test]
    fn test_summon_requirements_preserves_all_config_values() {
        // Arrange
        let mut config = Config::default();
        config.engine.sleep_tick_rate = 150;
        config.service.can_stop = false;
        config.pipes.task_pipe_capacity = 50;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert_eq!(ctx.config.engine.sleep_tick_rate, 150);
        assert!(!ctx.config.service.can_stop);
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 50);
    }

    #[test]
    fn test_summon_requirements_with_debug_enabled() {
        // Arrange
        let mut config = Config::default();
        config.debugging.is_debugging = true;
        config.debugging.visual_graph = true;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert!(ctx.config.debugging.is_debugging);
        assert!(ctx.config.debugging.visual_graph);
    }

    #[test]
    fn test_summon_requirements_with_large_pipe_capacity() {
        // Arrange
        let mut config = Config::default();
        config.pipes.task_pipe_capacity = 10000;
        config.pipes.notif_pipe_capacity = 10000;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 10000);
        assert_eq!(ctx.config.pipes.notif_pipe_capacity, 10000);
    }
}

mod tests_run_service {
    use thryve_run::run::run_service::create_service;
    use thryve_utils::cfg::config::Config;

    #[test]
    fn test_create_service_with_default_config() {
        // Arrange
        let config = Config::default();

        // Act
        let (service, _) = create_service(config);

        // Assert - service and ctx should be created successfully
        // The service struct has Arc<AtomicBool> fields which can't be directly compared
        assert!(service.is_running.load(std::sync::atomic::Ordering::SeqCst) == false);
        assert!(service.is_paused.load(std::sync::atomic::Ordering::SeqCst) == false);
    }

    #[test]
    fn test_create_service_with_custom_pipe_capacity() {
        // Arrange
        let mut config = Config::default();
        config.pipes.task_pipe_capacity = 100;
        config.pipes.notif_pipe_capacity = 200;

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 100);
        assert_eq!(ctx.config.pipes.notif_pipe_capacity, 200);
    }

    #[test]
    fn test_create_service_with_disabled_service_features() {
        // Arrange
        let mut config = Config::default();
        config.service.can_stop = false;
        config.service.can_pause = false;

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert!(!ctx.config.service.can_stop);
        assert!(!ctx.config.service.can_pause);
    }

    #[test]
    fn test_create_service_with_debug_enabled() {
        // Arrange
        let mut config = Config::default();
        config.debugging.is_debugging = true;
        config.debugging.visual_graph = true;

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert!(ctx.config.debugging.is_debugging);
        assert!(ctx.config.debugging.visual_graph);
    }

    #[test]
    fn test_create_service_preserves_all_config_values() {
        // Arrange
        let mut config = Config::default();
        config.engine.sleep_tick_rate = 150;
        config.service.can_stop = true;
        config.service.can_pause = true;
        config.pipes.task_pipe_capacity = 50;

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert_eq!(ctx.config.engine.sleep_tick_rate, 150);
        assert!(ctx.config.service.can_stop);
        assert!(ctx.config.service.can_pause);
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 50);
    }

    #[test]
    fn test_create_service_returns_valid_context() {
        // Arrange
        let config = Config::default();

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert!(!ctx.config.service.can_stop == false); // can_stop should be true
        assert!(!ctx.config.service.can_pause == false); // can_pause should be true
    }
}
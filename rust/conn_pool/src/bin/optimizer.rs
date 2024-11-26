use conn_pool::{knobs::*, test::spec::run_specs_tests_in_runtime};
use std::sync::{atomic::AtomicIsize, Mutex};

use genetic_algorithm::strategy::evolve::prelude::*;
use lru::LruCache;
use rand::Rng;

static LOG_LOCK: Mutex<()> = Mutex::new(());

fn main() {
    // Enable tracing
    // tracing_subscriber::fmt::init();

    const PREDICATE: fn(&str) -> bool = |_name| true;

    let qos = run_specs_tests_in_runtime(5, None, &PREDICATE).unwrap();
    println!("{qos:?}");

    // the search goal to optimize towards (maximize or minimize)
    #[derive(Clone, std::fmt::Debug, smart_default::SmartDefault)]
    pub struct Optimizer {
        #[default(std::sync::Arc::new(AtomicIsize::new(isize::MIN)))]
        best: std::sync::Arc<AtomicIsize>,
        #[default(LruCache::new(100_000_000.try_into().unwrap()))]
        lru: LruCache<[isize; ALL_KNOB_COUNT], isize>,
        #[default(std::time::Instant::now())]
        now: std::time::Instant,
    }

    impl Fitness for Optimizer {
        type Allele = isize;
        fn calculate_for_chromosome(
            &mut self,
            chromosome: &Chromosome<Self::Allele>,
        ) -> Option<FitnessValue> {
            let mut knobs: [isize; ALL_KNOB_COUNT] = Default::default();
            for (knob, gene) in knobs.iter_mut().zip(&chromosome.genes) {
                *knob = *gene as _;
            }
            if let Some(res) = self.lru.get(&knobs) {
                return Some(*res);
            }

            for (i, knob) in conn_pool::knobs::ALL_KNOBS.iter().enumerate() {
                if knob.set(knobs[i]).is_err() {
                    return None;
                };
            }

            let weights = [(1.0, 1, None), (1.0, 5, Some(10.0))];
            let outputs = weights
                .map(|(_, count, scale)| run_specs_tests_in_runtime(count, scale, &PREDICATE));
            let mut score = 0.0;
            for ((weight, ..), output) in weights.iter().zip(&outputs) {
                score += weight * output.as_ref().ok()?.qos_rms_error();
            }
            let qos_i = (score * 1_000_000.0) as isize;
            if qos_i > self.best.load(std::sync::atomic::Ordering::SeqCst) {
                let _lock = LOG_LOCK.lock();
                println!("{:?} New best: {score:.02} {knobs:?}", self.now.elapsed());
                println!("{:?}", conn_pool::knobs::ALL_KNOBS);
                for (weight, output) in weights.iter().zip(outputs) {
                    println!("{weight:?}: {:?}", output.ok()?);
                }
                println!("*****************************");
                self.best.store(qos_i, std::sync::atomic::Ordering::SeqCst);
            }
            self.lru.push(knobs, qos_i);

            Some(qos_i)
        }
    }

    let mut seeds: Vec<Vec<isize>> = vec![];

    // The current state
    seeds.push(
        conn_pool::knobs::ALL_KNOBS
            .iter()
            .map(|k| k.get() as _)
            .collect(),
    );

    // Some randomness
    for _ in 0..100 {
        seeds.push(
            conn_pool::knobs::ALL_KNOBS
                .iter()
                .map(|k| {
                    let proposed: isize =
                        (k.get() as f32 * rand::thread_rng().gen_range(-2.0..2.0_f32)) as _;
                    proposed
                })
                .collect(),
        );
    }

    let mut final_seeds = vec![];
    for mut seed in seeds {
        for (i, knob) in conn_pool::knobs::ALL_KNOBS.iter().enumerate() {
            let mut value = seed[i] as _;
            if knob.set(value).is_err() {
                knob.clamp(&mut value);
                seed[i] = value as _;
            };
        }
        final_seeds.push(seed);
    }

    let genotype = RangeGenotype::builder()
        .with_genes_size(conn_pool::knobs::ALL_KNOBS.len())
        .with_allele_range(-100_000..=100_000)
        .with_allele_mutation_range(-1000..=1000)
        .with_seed_genes_list(final_seeds)
        .build()
        .unwrap();

    let mut rng = rand::thread_rng(); // a randomness provider implementing Trait rand::Rng
    let evolve = Evolve::builder()
        .with_multithreading(true)
        .with_genotype(genotype)
        .with_target_population_size(1000)
        .with_target_fitness_score(100 * 1_000_000)
        .with_max_stale_generations(1000)
        .with_mutate(MutateMultiGeneDynamic::new(3, 0.2, 500))
        .with_fitness(Optimizer::default())
        .with_crossover(CrossoverUniform::new(true))
        .with_compete(CompeteTournament::new(200))
        .with_extension(ExtensionMassInvasion::new(200, 0.8))
        .with_reporter(EvolveReporterSimple::new(10))
        .call(&mut rng)
        .unwrap();
    println!("{}", evolve);
}

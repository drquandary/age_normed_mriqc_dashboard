"""
Database module for normative data, age groups, and quality thresholds.
"""

import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json
from contextlib import contextmanager

from .common_utils.logging_config import setup_logging

logger = setup_logging(__name__)


class NormativeDatabase:
    """Manages SQLite database for normative data and age groups."""
    
    def __init__(self, db_path: str = "data/normative_data.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        try:
            yield conn
        finally:
            conn.close()
    
    def _initialize_database(self):
        """Initialize database with schema and default data."""
        with self.get_connection() as conn:
            self._create_tables(conn)
            self._populate_default_data(conn)
            conn.commit()
    
    def _create_tables(self, conn: sqlite3.Connection):
        """Create database tables."""
        
        # Age groups table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS age_groups (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                min_age REAL NOT NULL,
                max_age REAL NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Normative data table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS normative_data (
                id INTEGER PRIMARY KEY,
                metric_name TEXT NOT NULL,
                age_group_id INTEGER NOT NULL,
                mean_value REAL NOT NULL,
                std_value REAL NOT NULL,
                percentile_5 REAL,
                percentile_25 REAL,
                percentile_50 REAL,
                percentile_75 REAL,
                percentile_95 REAL,
                sample_size INTEGER,
                dataset_source TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (age_group_id) REFERENCES age_groups(id),
                UNIQUE(metric_name, age_group_id)
            )
        """)
        
        # Quality thresholds table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quality_thresholds (
                id INTEGER PRIMARY KEY,
                metric_name TEXT NOT NULL,
                age_group_id INTEGER NOT NULL,
                warning_threshold REAL,
                fail_threshold REAL,
                direction TEXT NOT NULL CHECK (direction IN ('higher_better', 'lower_better')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (age_group_id) REFERENCES age_groups(id),
                UNIQUE(metric_name, age_group_id)
            )
        """)
        
        # Study configurations table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS study_configurations (
                id INTEGER PRIMARY KEY,
                study_name TEXT NOT NULL UNIQUE,
                normative_dataset TEXT NOT NULL DEFAULT 'default',
                exclusion_criteria TEXT,  -- JSON array
                created_by TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        """)
        
        # Custom age groups table (linked to study configurations)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS custom_age_groups (
                id INTEGER PRIMARY KEY,
                study_config_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                min_age REAL NOT NULL,
                max_age REAL NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (study_config_id) REFERENCES study_configurations(id) ON DELETE CASCADE,
                UNIQUE(study_config_id, name)
            )
        """)
        
        # Custom quality thresholds table (linked to study configurations)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS custom_quality_thresholds (
                id INTEGER PRIMARY KEY,
                study_config_id INTEGER NOT NULL,
                metric_name TEXT NOT NULL,
                age_group_name TEXT NOT NULL,
                warning_threshold REAL,
                fail_threshold REAL,
                direction TEXT NOT NULL CHECK (direction IN ('higher_better', 'lower_better')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (study_config_id) REFERENCES study_configurations(id) ON DELETE CASCADE,
                UNIQUE(study_config_id, metric_name, age_group_name)
            )
        """)
        
        # Create indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_normative_metric_age ON normative_data(metric_name, age_group_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_thresholds_metric_age ON quality_thresholds(metric_name, age_group_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_age_groups_range ON age_groups(min_age, max_age)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_study_configs_name ON study_configurations(study_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_custom_age_groups_study ON custom_age_groups(study_config_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_custom_thresholds_study ON custom_quality_thresholds(study_config_id)")
    
    def _populate_default_data(self, conn: sqlite3.Connection):
        """Populate database with default age groups and normative data."""
        
        # Check if data already exists
        cursor = conn.execute("SELECT COUNT(*) FROM age_groups")
        if cursor.fetchone()[0] > 0:
            return  # Data already exists
        
        # Default age groups
        age_groups = [
            ("pediatric", 6.0, 12.0, "Pediatric population (6-12 years)"),
            ("adolescent", 13.0, 17.0, "Adolescent population (13-17 years)"),
            ("young_adult", 18.0, 35.0, "Young adult population (18-35 years)"),
            ("middle_age", 36.0, 65.0, "Middle-aged population (36-65 years)"),
            ("elderly", 66.0, 100.0, "Elderly population (65+ years)")
        ]
        
        conn.executemany("""
            INSERT INTO age_groups (name, min_age, max_age, description)
            VALUES (?, ?, ?, ?)
        """, age_groups)
        
        # Default normative data (based on literature values)
        self._populate_normative_data(conn)
        self._populate_quality_thresholds(conn)
    
    def _populate_normative_data(self, conn: sqlite3.Connection):
        """Populate normative data for different age groups."""
        
        # Get age group IDs
        age_group_ids = {}
        cursor = conn.execute("SELECT id, name FROM age_groups")
        for row in cursor:
            age_group_ids[row['name']] = row['id']
        
        # Normative data for anatomical metrics (example values based on literature)
        normative_data = [
            # SNR values
            ("snr", "pediatric", 15.2, 3.1, 10.5, 13.2, 15.1, 17.3, 20.8, 150),
            ("snr", "adolescent", 16.8, 2.9, 12.1, 14.8, 16.7, 18.9, 22.1, 200),
            ("snr", "young_adult", 18.5, 2.7, 14.2, 16.8, 18.4, 20.3, 23.2, 300),
            ("snr", "middle_age", 17.9, 3.2, 12.8, 15.9, 17.8, 19.8, 23.5, 250),
            ("snr", "elderly", 16.1, 3.8, 10.2, 13.8, 16.0, 18.5, 22.1, 180),
            
            # CNR values
            ("cnr", "pediatric", 3.8, 0.9, 2.3, 3.2, 3.8, 4.4, 5.2, 150),
            ("cnr", "adolescent", 4.2, 0.8, 2.8, 3.6, 4.2, 4.8, 5.6, 200),
            ("cnr", "young_adult", 4.6, 0.7, 3.4, 4.1, 4.6, 5.1, 5.8, 300),
            ("cnr", "middle_age", 4.3, 0.9, 2.8, 3.7, 4.3, 4.9, 5.7, 250),
            ("cnr", "elderly", 3.9, 1.1, 2.1, 3.2, 3.9, 4.6, 5.5, 180),
            
            # FBER values
            ("fber", "pediatric", 1420.0, 280.0, 950.0, 1220.0, 1410.0, 1620.0, 1890.0, 150),
            ("fber", "adolescent", 1580.0, 260.0, 1150.0, 1380.0, 1570.0, 1780.0, 2050.0, 200),
            ("fber", "young_adult", 1750.0, 240.0, 1350.0, 1580.0, 1740.0, 1920.0, 2180.0, 300),
            ("fber", "middle_age", 1680.0, 290.0, 1200.0, 1480.0, 1670.0, 1880.0, 2160.0, 250),
            ("fber", "elderly", 1520.0, 340.0, 980.0, 1280.0, 1510.0, 1760.0, 2080.0, 180),
            
            # EFC values (lower is better)
            ("efc", "pediatric", 0.52, 0.08, 0.38, 0.47, 0.52, 0.57, 0.66, 150),
            ("efc", "adolescent", 0.48, 0.07, 0.36, 0.43, 0.48, 0.53, 0.61, 200),
            ("efc", "young_adult", 0.45, 0.06, 0.34, 0.41, 0.45, 0.49, 0.56, 300),
            ("efc", "middle_age", 0.47, 0.08, 0.33, 0.42, 0.47, 0.52, 0.61, 250),
            ("efc", "elderly", 0.51, 0.09, 0.36, 0.45, 0.51, 0.57, 0.67, 180),
            
            # FWHM values (lower is better)
            ("fwhm_avg", "pediatric", 2.95, 0.35, 2.35, 2.70, 2.94, 3.20, 3.55, 150),
            ("fwhm_avg", "adolescent", 2.82, 0.32, 2.28, 2.58, 2.81, 3.06, 3.38, 200),
            ("fwhm_avg", "young_adult", 2.75, 0.28, 2.25, 2.54, 2.74, 2.96, 3.25, 300),
            ("fwhm_avg", "middle_age", 2.88, 0.34, 2.30, 2.62, 2.87, 3.14, 3.46, 250),
            ("fwhm_avg", "elderly", 3.12, 0.42, 2.45, 2.82, 3.11, 3.42, 3.85, 180),
        ]
        
        # Insert normative data
        for metric_name, age_group, mean_val, std_val, p5, p25, p50, p75, p95, n in normative_data:
            age_group_id = age_group_ids[age_group]
            conn.execute("""
                INSERT OR REPLACE INTO normative_data 
                (metric_name, age_group_id, mean_value, std_value, 
                 percentile_5, percentile_25, percentile_50, percentile_75, percentile_95, 
                 sample_size, dataset_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (metric_name, age_group_id, mean_val, std_val, p5, p25, p50, p75, p95, n, "literature_composite"))
    
    def _populate_quality_thresholds(self, conn: sqlite3.Connection):
        """Populate quality thresholds for different metrics and age groups."""
        
        # Get age group IDs
        age_group_ids = {}
        cursor = conn.execute("SELECT id, name FROM age_groups")
        for row in cursor:
            age_group_ids[row['name']] = row['id']
        
        # Quality thresholds (warning and fail thresholds)
        thresholds = [
            # SNR thresholds (higher is better)
            ("snr", "pediatric", 12.0, 8.0, "higher_better"),
            ("snr", "adolescent", 13.0, 9.0, "higher_better"),
            ("snr", "young_adult", 14.0, 10.0, "higher_better"),
            ("snr", "middle_age", 13.5, 9.5, "higher_better"),
            ("snr", "elderly", 12.5, 8.5, "higher_better"),
            
            # CNR thresholds (higher is better)
            ("cnr", "pediatric", 2.8, 2.0, "higher_better"),
            ("cnr", "adolescent", 3.2, 2.4, "higher_better"),
            ("cnr", "young_adult", 3.6, 2.8, "higher_better"),
            ("cnr", "middle_age", 3.4, 2.6, "higher_better"),
            ("cnr", "elderly", 3.0, 2.2, "higher_better"),
            
            # EFC thresholds (lower is better)
            ("efc", "pediatric", 0.60, 0.70, "lower_better"),
            ("efc", "adolescent", 0.55, 0.65, "lower_better"),
            ("efc", "young_adult", 0.52, 0.60, "lower_better"),
            ("efc", "middle_age", 0.55, 0.65, "lower_better"),
            ("efc", "elderly", 0.62, 0.72, "lower_better"),
            
            # FWHM thresholds (lower is better)
            ("fwhm_avg", "pediatric", 3.4, 3.8, "lower_better"),
            ("fwhm_avg", "adolescent", 3.2, 3.6, "lower_better"),
            ("fwhm_avg", "young_adult", 3.1, 3.4, "lower_better"),
            ("fwhm_avg", "middle_age", 3.3, 3.7, "lower_better"),
            ("fwhm_avg", "elderly", 3.6, 4.0, "lower_better"),
        ]
        
        # Insert thresholds
        for metric_name, age_group, warn_thresh, fail_thresh, direction in thresholds:
            age_group_id = age_group_ids[age_group]
            conn.execute("""
                INSERT OR REPLACE INTO quality_thresholds 
                (metric_name, age_group_id, warning_threshold, fail_threshold, direction)
                VALUES (?, ?, ?, ?, ?)
            """, (metric_name, age_group_id, warn_thresh, fail_thresh, direction))
    
    def get_age_groups(self) -> List[Dict]:
        """Get all age groups."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT id, name, min_age, max_age, description 
                FROM age_groups 
                ORDER BY min_age
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_age_group_by_age(self, age: float) -> Optional[Dict]:
        """Get age group for a specific age."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT id, name, min_age, max_age, description 
                FROM age_groups 
                WHERE ? >= min_age AND ? <= max_age
            """, (age, age))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_normative_data(self, metric_name: str, age_group_id: int) -> Optional[Dict]:
        """Get normative data for a specific metric and age group."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM normative_data 
                WHERE metric_name = ? AND age_group_id = ?
            """, (metric_name, age_group_id))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_quality_thresholds(self, metric_name: str, age_group_id: int) -> Optional[Dict]:
        """Get quality thresholds for a specific metric and age group."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM quality_thresholds 
                WHERE metric_name = ? AND age_group_id = ?
            """, (metric_name, age_group_id))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def add_custom_normative_data(self, metric_name: str, age_group_id: int, 
                                 mean_value: float, std_value: float,
                                 percentiles: Dict[str, float], 
                                 sample_size: int, dataset_source: str):
        """Add custom normative data."""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO normative_data 
                (metric_name, age_group_id, mean_value, std_value,
                 percentile_5, percentile_25, percentile_50, percentile_75, percentile_95,
                 sample_size, dataset_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (metric_name, age_group_id, mean_value, std_value,
                  percentiles.get('5', None), percentiles.get('25', None),
                  percentiles.get('50', None), percentiles.get('75', None),
                  percentiles.get('95', None), sample_size, dataset_source))
            conn.commit()
    
    def add_custom_age_group(self, name: str, min_age: float, max_age: float, 
                           description: str = None) -> int:
        """Add custom age group."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO age_groups (name, min_age, max_age, description)
                VALUES (?, ?, ?, ?)
            """, (name, min_age, max_age, description))
            conn.commit()
            return cursor.lastrowid
    
    # Study Configuration Management Methods
    
    def create_study_configuration(self, study_name: str, normative_dataset: str = "default",
                                 exclusion_criteria: List[str] = None, created_by: str = "system") -> int:
        """Create a new study configuration."""
        with self.get_connection() as conn:
            exclusion_json = json.dumps(exclusion_criteria or [])
            cursor = conn.execute("""
                INSERT INTO study_configurations 
                (study_name, normative_dataset, exclusion_criteria, created_by)
                VALUES (?, ?, ?, ?)
            """, (study_name, normative_dataset, exclusion_json, created_by))
            conn.commit()
            return cursor.lastrowid
    
    def get_study_configuration(self, study_name: str) -> Optional[Dict]:
        """Get study configuration by name."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM study_configurations 
                WHERE study_name = ? AND is_active = 1
            """, (study_name,))
            row = cursor.fetchone()
            if not row:
                return None
            
            config = dict(row)
            config['exclusion_criteria'] = json.loads(config['exclusion_criteria'] or '[]')
            
            # Get custom age groups
            cursor = conn.execute("""
                SELECT name, min_age, max_age, description 
                FROM custom_age_groups 
                WHERE study_config_id = ?
                ORDER BY min_age
            """, (config['id'],))
            config['custom_age_groups'] = [dict(row) for row in cursor.fetchall()]
            
            # Get custom thresholds
            cursor = conn.execute("""
                SELECT metric_name, age_group_name, warning_threshold, fail_threshold, direction
                FROM custom_quality_thresholds 
                WHERE study_config_id = ?
            """, (config['id'],))
            config['custom_thresholds'] = [dict(row) for row in cursor.fetchall()]
            
            return config
    
    def get_all_study_configurations(self) -> List[Dict]:
        """Get all active study configurations."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT id, study_name, normative_dataset, created_by, created_at, updated_at
                FROM study_configurations 
                WHERE is_active = 1
                ORDER BY created_at DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def update_study_configuration(self, study_name: str, normative_dataset: str = None,
                                 exclusion_criteria: List[str] = None) -> bool:
        """Update an existing study configuration."""
        with self.get_connection() as conn:
            updates = []
            params = []
            
            if normative_dataset is not None:
                updates.append("normative_dataset = ?")
                params.append(normative_dataset)
            
            if exclusion_criteria is not None:
                updates.append("exclusion_criteria = ?")
                params.append(json.dumps(exclusion_criteria))
            
            if updates:
                updates.append("updated_at = CURRENT_TIMESTAMP")
                params.append(study_name)
                
                cursor = conn.execute(f"""
                    UPDATE study_configurations 
                    SET {', '.join(updates)}
                    WHERE study_name = ? AND is_active = 1
                """, params)
                conn.commit()
                return cursor.rowcount > 0
            
            return False
    
    def delete_study_configuration(self, study_name: str) -> bool:
        """Soft delete a study configuration."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                UPDATE study_configurations 
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                WHERE study_name = ? AND is_active = 1
            """, (study_name,))
            conn.commit()
            return cursor.rowcount > 0
    
    def add_custom_age_group_to_study(self, study_name: str, name: str, min_age: float, 
                                    max_age: float, description: str = None) -> bool:
        """Add custom age group to a study configuration."""
        with self.get_connection() as conn:
            # Get study config ID
            cursor = conn.execute("""
                SELECT id FROM study_configurations 
                WHERE study_name = ? AND is_active = 1
            """, (study_name,))
            row = cursor.fetchone()
            if not row:
                return False
            
            study_config_id = row['id']
            
            # Add custom age group
            try:
                conn.execute("""
                    INSERT INTO custom_age_groups 
                    (study_config_id, name, min_age, max_age, description)
                    VALUES (?, ?, ?, ?, ?)
                """, (study_config_id, name, min_age, max_age, description))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                # Age group name already exists for this study
                return False
    
    def add_custom_threshold_to_study(self, study_name: str, metric_name: str, 
                                    age_group_name: str, warning_threshold: float,
                                    fail_threshold: float, direction: str) -> bool:
        """Add custom quality threshold to a study configuration."""
        with self.get_connection() as conn:
            # Get study config ID
            cursor = conn.execute("""
                SELECT id FROM study_configurations 
                WHERE study_name = ? AND is_active = 1
            """, (study_name,))
            row = cursor.fetchone()
            if not row:
                return False
            
            study_config_id = row['id']
            
            # Validate direction
            if direction not in ['higher_better', 'lower_better']:
                return False
            
            # Add custom threshold
            try:
                conn.execute("""
                    INSERT INTO custom_quality_thresholds 
                    (study_config_id, metric_name, age_group_name, warning_threshold, 
                     fail_threshold, direction)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (study_config_id, metric_name, age_group_name, warning_threshold, 
                      fail_threshold, direction))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                # Threshold already exists for this metric/age group/study
                return False
    
    def get_custom_age_groups_for_study(self, study_name: str) -> List[Dict]:
        """Get custom age groups for a study."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT cag.name, cag.min_age, cag.max_age, cag.description
                FROM custom_age_groups cag
                JOIN study_configurations sc ON cag.study_config_id = sc.id
                WHERE sc.study_name = ? AND sc.is_active = 1
                ORDER BY cag.min_age
            """, (study_name,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_custom_thresholds_for_study(self, study_name: str) -> List[Dict]:
        """Get custom quality thresholds for a study."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT cqt.metric_name, cqt.age_group_name, cqt.warning_threshold,
                       cqt.fail_threshold, cqt.direction
                FROM custom_quality_thresholds cqt
                JOIN study_configurations sc ON cqt.study_config_id = sc.id
                WHERE sc.study_name = ? AND sc.is_active = 1
            """, (study_name,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_effective_age_groups_for_study(self, study_name: str) -> List[Dict]:
        """Get effective age groups for a study (custom + default)."""
        custom_groups = self.get_custom_age_groups_for_study(study_name)
        if custom_groups:
            return custom_groups
        else:
            return self.get_age_groups()
    
    def get_effective_thresholds_for_study(self, study_name: str, metric_name: str, 
                                         age_group_name: str) -> Optional[Dict]:
        """Get effective quality thresholds for a study (custom or default)."""
        with self.get_connection() as conn:
            # Try custom thresholds first
            cursor = conn.execute("""
                SELECT cqt.warning_threshold, cqt.fail_threshold, cqt.direction
                FROM custom_quality_thresholds cqt
                JOIN study_configurations sc ON cqt.study_config_id = sc.id
                WHERE sc.study_name = ? AND sc.is_active = 1 
                  AND cqt.metric_name = ? AND cqt.age_group_name = ?
            """, (study_name, metric_name, age_group_name))
            row = cursor.fetchone()
            if row:
                return dict(row)
            
            # Fall back to default thresholds
            cursor = conn.execute("""
                SELECT qt.warning_threshold, qt.fail_threshold, qt.direction
                FROM quality_thresholds qt
                JOIN age_groups ag ON qt.age_group_id = ag.id
                WHERE qt.metric_name = ? AND ag.name = ?
            """, (metric_name, age_group_name))
            row = cursor.fetchone()
            return dict(row) if row else None
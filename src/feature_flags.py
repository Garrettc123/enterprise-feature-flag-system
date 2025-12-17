"""Enterprise Feature Flag System

Real-time feature flags with targeting, gradual rollouts, A/B testing,
kill switches, and comprehensive analytics.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib
import json

logger = logging.getLogger(__name__)


class RolloutStrategy(Enum):
    ALL_USERS = "all_users"
    PERCENTAGE = "percentage"
    TARGETED = "targeted"
    GRADUAL = "gradual"
    CANARY = "canary"


@dataclass
class FeatureFlag:
    key: str
    name: str
    description: str
    enabled: bool = False
    rollout_strategy: RolloutStrategy = RolloutStrategy.ALL_USERS
    rollout_percentage: float = 0.0
    target_users: Set[str] = field(default_factory=set)
    target_groups: Set[str] = field(default_factory=set)
    target_attributes: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    evaluations: int = 0
    enabled_count: int = 0


@dataclass
class User:
    id: str
    email: str
    groups: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)


class TargetingEngine:
    """Evaluate targeting rules for feature flags"""
    
    def __init__(self):
        self.evaluations = 0
        
    def evaluate(self, flag: FeatureFlag, user: User) -> bool:
        """Evaluate if flag is enabled for user"""
        self.evaluations += 1
        flag.evaluations += 1
        
        if not flag.enabled:
            return False
            
        if flag.rollout_strategy == RolloutStrategy.ALL_USERS:
            result = True
        elif flag.rollout_strategy == RolloutStrategy.TARGETED:
            result = self._evaluate_targeted(flag, user)
        elif flag.rollout_strategy == RolloutStrategy.PERCENTAGE:
            result = self._evaluate_percentage(flag, user)
        elif flag.rollout_strategy == RolloutStrategy.GRADUAL:
            result = self._evaluate_gradual(flag, user)
        elif flag.rollout_strategy == RolloutStrategy.CANARY:
            result = self._evaluate_canary(flag, user)
        else:
            result = False
            
        if result:
            flag.enabled_count += 1
            
        return result
        
    def _evaluate_targeted(self, flag: FeatureFlag, user: User) -> bool:
        """Check if user is in target list"""
        if user.id in flag.target_users:
            return True
            
        if any(group in flag.target_groups for group in user.groups):
            return True
            
        # Check attribute targeting
        for attr, value in flag.target_attributes.items():
            if user.attributes.get(attr) == value:
                return True
                
        return False
        
    def _evaluate_percentage(self, flag: FeatureFlag, user: User) -> bool:
        """Percentage-based rollout"""
        # Consistent hashing for stable rollout
        hash_input = f"{flag.key}:{user.id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        percentage = (hash_value % 100) / 100.0
        
        return percentage < flag.rollout_percentage
        
    def _evaluate_gradual(self, flag: FeatureFlag, user: User) -> bool:
        """Gradual rollout (percentage increases over time)"""
        elapsed_hours = (datetime.now() - flag.created_at).total_seconds() / 3600
        target_percentage = min(1.0, elapsed_hours * 0.1)  # 10% per hour
        
        hash_input = f"{flag.key}:{user.id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        percentage = (hash_value % 100) / 100.0
        
        return percentage < target_percentage
        
    def _evaluate_canary(self, flag: FeatureFlag, user: User) -> bool:
        """Canary deployment (small percentage of production users)"""
        return self._evaluate_percentage(flag, user) and flag.rollout_percentage <= 0.05


class FeatureFlagManager:
    """Manage feature flags"""
    
    def __init__(self):
        self.flags: Dict[str, FeatureFlag] = {}
        self.targeting_engine = TargetingEngine()
        
    def create_flag(self, key: str, name: str, description: str) -> FeatureFlag:
        """Create new feature flag"""
        flag = FeatureFlag(
            key=key,
            name=name,
            description=description
        )
        self.flags[key] = flag
        logger.info(f"Created feature flag: {name}")
        return flag
        
    def update_flag(self, key: str, **kwargs):
        """Update feature flag"""
        if key not in self.flags:
            return False
            
        flag = self.flags[key]
        for k, v in kwargs.items():
            if hasattr(flag, k):
                setattr(flag, k, v)
        flag.updated_at = datetime.now()
        
        logger.info(f"Updated flag {key}: {kwargs}")
        return True
        
    def is_enabled(self, key: str, user: User) -> bool:
        """Check if flag is enabled for user"""
        if key not in self.flags:
            logger.warning(f"Flag {key} not found, returning False")
            return False
            
        return self.targeting_engine.evaluate(self.flags[key], user)
        
    def rollout_gradually(self, key: str, target_percentage: float, step: float = 0.1):
        """Gradually increase rollout percentage"""
        if key not in self.flags:
            return
            
        flag = self.flags[key]
        flag.rollout_strategy = RolloutStrategy.PERCENTAGE
        
        current = flag.rollout_percentage
        while current < target_percentage:
            current = min(current + step, target_percentage)
            flag.rollout_percentage = current
            logger.info(f"Flag {key} rolled out to {current:.1%}")


class KillSwitch:
    """Emergency kill switches for features"""
    
    def __init__(self, manager: FeatureFlagManager):
        self.manager = manager
        self.activated_switches: List[Dict[str, Any]] = []
        
    async def activate(self, flag_key: str, reason: str):
        """Activate kill switch for feature"""
        if flag_key in self.manager.flags:
            flag = self.manager.flags[flag_key]
            flag.enabled = False
            
            self.activated_switches.append({
                'flag': flag_key,
                'reason': reason,
                'timestamp': datetime.now()
            })
            
            logger.critical(f"KILL SWITCH ACTIVATED: {flag_key} - {reason}")
            return True
        return False
        
    async def deactivate(self, flag_key: str):
        """Deactivate kill switch"""
        if flag_key in self.manager.flags:
            flag = self.manager.flags[flag_key]
            flag.enabled = True
            logger.info(f"Kill switch deactivated: {flag_key}")
            return True
        return False


class ABTestingEngine:
    """A/B testing with feature flags"""
    
    def __init__(self):
        self.experiments: Dict[str, Dict[str, Any]] = {}
        
    def create_experiment(self, name: str, flag_a: str, flag_b: str, split: float = 0.5):
        """Create A/B test experiment"""
        exp_id = f"exp-{len(self.experiments)}"
        self.experiments[exp_id] = {
            'name': name,
            'flag_a': flag_a,
            'flag_b': flag_b,
            'split': split,
            'variant_a_count': 0,
            'variant_b_count': 0,
            'variant_a_conversions': 0,
            'variant_b_conversions': 0
        }
        logger.info(f"Created A/B experiment: {name}")
        return exp_id
        
    def assign_variant(self, exp_id: str, user: User) -> str:
        """Assign user to variant"""
        if exp_id not in self.experiments:
            return 'a'
            
        exp = self.experiments[exp_id]
        hash_input = f"{exp_id}:{user.id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        
        variant = 'a' if (hash_value % 100) / 100.0 < exp['split'] else 'b'
        
        if variant == 'a':
            exp['variant_a_count'] += 1
        else:
            exp['variant_b_count'] += 1
            
        return variant
        
    def record_conversion(self, exp_id: str, user: User):
        """Record conversion for experiment"""
        if exp_id not in self.experiments:
            return
            
        variant = self.assign_variant(exp_id, user)
        exp = self.experiments[exp_id]
        
        if variant == 'a':
            exp['variant_a_conversions'] += 1
        else:
            exp['variant_b_conversions'] += 1
            
    def get_results(self, exp_id: str) -> Dict[str, Any]:
        """Get experiment results"""
        if exp_id not in self.experiments:
            return {}
            
        exp = self.experiments[exp_id]
        
        conv_rate_a = exp['variant_a_conversions'] / max(exp['variant_a_count'], 1)
        conv_rate_b = exp['variant_b_conversions'] / max(exp['variant_b_count'], 1)
        
        return {
            'name': exp['name'],
            'variant_a': {
                'users': exp['variant_a_count'],
                'conversions': exp['variant_a_conversions'],
                'rate': conv_rate_a
            },
            'variant_b': {
                'users': exp['variant_b_count'],
                'conversions': exp['variant_b_conversions'],
                'rate': conv_rate_b
            },
            'winner': 'a' if conv_rate_a > conv_rate_b else 'b',
            'lift': abs(conv_rate_a - conv_rate_b) / max(conv_rate_a, conv_rate_b)
        }


class AnalyticsEngine:
    """Feature flag analytics and insights"""
    
    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        
    def track_evaluation(self, flag_key: str, user_id: str, enabled: bool):
        """Track flag evaluation"""
        self.events.append({
            'type': 'evaluation',
            'flag': flag_key,
            'user': user_id,
            'enabled': enabled,
            'timestamp': datetime.now()
        })
        
    def get_flag_stats(self, flag: FeatureFlag) -> Dict[str, Any]:
        """Get statistics for flag"""
        return {
            'key': flag.key,
            'evaluations': flag.evaluations,
            'enabled_count': flag.enabled_count,
            'enabled_rate': flag.enabled_count / max(flag.evaluations, 1),
            'rollout_percentage': flag.rollout_percentage,
            'target_users': len(flag.target_users)
        }


class FeatureFlagSystem:
    """Main enterprise feature flag system"""
    
    def __init__(self):
        self.manager = FeatureFlagManager()
        self.kill_switch = KillSwitch(self.manager)
        self.ab_testing = ABTestingEngine()
        self.analytics = AnalyticsEngine()
        
    async def demo(self):
        """Demonstration"""
        logger.info("\n" + "="*60)
        logger.info("ENTERPRISE FEATURE FLAG SYSTEM")
        logger.info("="*60)
        
        # Create flags
        new_ui = self.manager.create_flag(
            key="new_ui",
            name="New UI Design",
            description="Redesigned user interface"
        )
        new_ui.enabled = True
        new_ui.rollout_strategy = RolloutStrategy.PERCENTAGE
        new_ui.rollout_percentage = 0.25
        
        premium = self.manager.create_flag(
            key="premium_features",
            name="Premium Features",
            description="Advanced premium features"
        )
        premium.enabled = True
        premium.rollout_strategy = RolloutStrategy.TARGETED
        premium.target_groups = {'premium', 'enterprise'}
        
        # Create users
        users = [
            User(id="1", email="user1@test.com", groups=["free"]),
            User(id="2", email="user2@test.com", groups=["premium"]),
            User(id="3", email="user3@test.com", groups=["enterprise"]),
        ]
        
        # Test flag evaluations
        logger.info("\nEvaluating flags for users:")
        for user in users * 10:  # Test multiple times
            new_ui_enabled = self.manager.is_enabled("new_ui", user)
            premium_enabled = self.manager.is_enabled("premium_features", user)
            
            self.analytics.track_evaluation("new_ui", user.id, new_ui_enabled)
            self.analytics.track_evaluation("premium_features", user.id, premium_enabled)
            
        # A/B testing
        exp_id = self.ab_testing.create_experiment(
            name="New Checkout Flow",
            flag_a="checkout_v1",
            flag_b="checkout_v2"
        )
        
        # Simulate experiment
        import random
        for user in users * 100:
            variant = self.ab_testing.assign_variant(exp_id, user)
            if random.random() < 0.15:  # 15% conversion rate
                self.ab_testing.record_conversion(exp_id, user)
                
        # Get results
        results = self.ab_testing.get_results(exp_id)
        logger.info(f"\nA/B Test Results:")
        logger.info(f"  Winner: Variant {results['winner']}")
        logger.info(f"  Variant A: {results['variant_a']['rate']:.1%} conversion")
        logger.info(f"  Variant B: {results['variant_b']['rate']:.1%} conversion")
        logger.info(f"  Lift: {results['lift']:.1%}")
        
        # Kill switch demo
        logger.info(f"\nTesting kill switch...")
        await self.kill_switch.activate("new_ui", "Performance issues detected")
        
        # Analytics
        logger.info(f"\nFlag Analytics:")
        for flag in self.manager.flags.values():
            stats = self.analytics.get_flag_stats(flag)
            logger.info(f"  {flag.name}:")
            logger.info(f"    Evaluations: {stats['evaluations']}")
            logger.info(f"    Enabled Rate: {stats['enabled_rate']:.1%}")
            
        logger.info("\n" + "="*60)
        logger.info("FEATURE FLAG SYSTEM: OPERATIONAL")
        logger.info("="*60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    system = FeatureFlagSystem()
    asyncio.run(system.demo())

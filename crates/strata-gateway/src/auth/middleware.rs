//! Authentication middleware — Tower layer for request authentication.

/// Authentication context injected into request extensions.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub identity: String,
    pub role: Role,
}

/// User roles for RBAC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Admin,
    Writer,
    Reader,
    Agent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_equality() {
        assert_eq!(Role::Admin, Role::Admin);
        assert_ne!(Role::Admin, Role::Reader);
        assert_ne!(Role::Writer, Role::Agent);
    }

    #[test]
    fn auth_context_clone() {
        let ctx = AuthContext {
            identity: "user-1".into(),
            role: Role::Admin,
        };
        let cloned = ctx.clone();
        assert_eq!(cloned.identity, "user-1");
        assert_eq!(cloned.role, Role::Admin);
    }

    #[test]
    fn auth_context_debug() {
        let ctx = AuthContext {
            identity: "agent-bot".into(),
            role: Role::Agent,
        };
        let debug = format!("{:?}", ctx);
        assert!(debug.contains("agent-bot"));
        assert!(debug.contains("Agent"));
    }

    #[test]
    fn all_roles_are_distinct() {
        let roles = [Role::Admin, Role::Writer, Role::Reader, Role::Agent];
        for (i, a) in roles.iter().enumerate() {
            for (j, b) in roles.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }
}
